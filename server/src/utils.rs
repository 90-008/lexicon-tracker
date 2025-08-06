use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use byteview::ByteView;
use ordered_varint::Variable;

pub fn get_time() -> Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
}

pub trait WriteVariableExt: Write {
    fn write_varint(&mut self, value: impl Variable) -> io::Result<usize> {
        value.encode_variable(self)
    }
}
impl<W: Write> WriteVariableExt for W {}

pub trait ReadVariableExt: Read {
    fn read_varint<T: Variable>(&mut self) -> io::Result<T> {
        T::decode_variable(self)
    }
}
impl<R: Read> ReadVariableExt for R {}

pub struct WritableByteView {
    view: ByteView,
    written: usize,
}

impl WritableByteView {
    // returns None if the view already has a reference to it
    pub fn with_size(capacity: usize) -> Self {
        Self {
            view: ByteView::with_size(capacity),
            written: 0,
        }
    }

    #[inline(always)]
    pub fn into_inner(self) -> ByteView {
        self.view
    }
}

impl Write for WritableByteView {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        if len > self.view.len() - self.written {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "buffer full",
            ));
        }
        // SAFETY: this is safe because we have checked that the buffer is not full
        // SAFETY: we own the mutator so no other references to the view exist
        unsafe {
            std::ptr::copy_nonoverlapping(
                buf.as_ptr(),
                self.view
                    .get_mut()
                    .unwrap_unchecked()
                    .as_mut_ptr()
                    .add(self.written),
                len,
            );
            self.written += len;
        }
        Ok(len)
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn varints_unsigned_encoded<const N: usize>(values: [u64; N]) -> ByteView {
    let mut buf =
        WritableByteView::with_size(values.into_iter().map(varint_unsigned_encoded_len).sum());
    for value in values {
        // cant fail
        let _ = buf.write_varint(value);
    }
    buf.into_inner()
}

// gets the encoded length of a varint-encoded unsigned integer
// see ordered_varint
pub fn varint_unsigned_encoded_len(value: u64) -> usize {
    let value = value.to_be_bytes();
    value
        .iter()
        .enumerate()
        .find_map(|(index, &byte)| {
            (byte > 0).then(|| {
                let extra_bytes = 7 - index;
                (byte < 16)
                    .then(|| extra_bytes + 1)
                    .unwrap_or_else(|| extra_bytes + 2)
            })
        })
        .unwrap_or(0)
        .max(1)
}

pub static CLOCK: std::sync::LazyLock<quanta::Clock> =
    std::sync::LazyLock::new(|| quanta::Clock::new());

/// simple thread-safe rate tracker using time buckets
/// divides time into fixed buckets and rotates through them
#[derive(Debug)]
pub struct RateTracker<const BUCKET_WINDOW: u64> {
    buckets: Vec<AtomicU64>,
    last_bucket_time: AtomicU64,
    bucket_duration_nanos: u64,
    window_duration: Duration,
    start_time: u64, // raw time when tracker was created
}

pub type DefaultRateTracker = RateTracker<1000>;

impl<const BUCKET_WINDOW: u64> RateTracker<BUCKET_WINDOW> {
    /// create a new rate tracker with the specified time window
    pub fn new(window_duration: Duration) -> Self {
        let bucket_duration_nanos = Duration::from_millis(BUCKET_WINDOW).as_nanos() as u64;
        let num_buckets =
            (window_duration.as_nanos() as u64 / bucket_duration_nanos).max(1) as usize;

        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            buckets.push(AtomicU64::new(0));
        }

        let start_time = CLOCK.raw();
        Self {
            buckets,
            bucket_duration_nanos,
            window_duration,
            last_bucket_time: AtomicU64::new(0),
            start_time,
        }
    }

    #[inline(always)]
    fn elapsed(&self) -> u64 {
        CLOCK.delta_as_nanos(self.start_time, CLOCK.raw())
    }

    /// record an event
    pub fn observe(&self) {
        self.maybe_advance_buckets();

        let bucket_index = self.get_current_bucket_index();
        self.buckets[bucket_index].fetch_add(1, Ordering::Relaxed);
    }

    /// get the current rate in events per second
    pub fn rate(&self) -> f64 {
        self.maybe_advance_buckets();

        let total_events: u64 = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .sum();

        total_events as f64 / self.window_duration.as_secs_f64()
    }

    fn get_current_bucket_index(&self) -> usize {
        let bucket_number = self.elapsed() / self.bucket_duration_nanos;
        (bucket_number as usize) % self.buckets.len()
    }

    fn maybe_advance_buckets(&self) {
        let current_bucket_time =
            (self.elapsed() / self.bucket_duration_nanos) * self.bucket_duration_nanos;
        let last_bucket_time = self.last_bucket_time.load(Ordering::Relaxed);

        if current_bucket_time > last_bucket_time {
            // try to update the last bucket time
            if self
                .last_bucket_time
                .compare_exchange_weak(
                    last_bucket_time,
                    current_bucket_time,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // clear buckets that are now too old
                let buckets_to_advance = ((current_bucket_time - last_bucket_time)
                    / self.bucket_duration_nanos)
                    .min(self.buckets.len() as u64);

                for i in 0..buckets_to_advance {
                    let bucket_time = last_bucket_time + (i + 1) * self.bucket_duration_nanos;
                    let bucket_index =
                        (bucket_time / self.bucket_duration_nanos) as usize % self.buckets.len();
                    self.buckets[bucket_index].store(0, Ordering::Relaxed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_rate_tracker_basic() {
        let tracker = DefaultRateTracker::new(Duration::from_secs(2));

        // record some events
        tracker.observe();
        tracker.observe();
        tracker.observe();

        let rate = tracker.rate();
        assert_eq!(rate, 1.5); // 3 events over 2 seconds = 1.5 events/sec
    }

    #[test]
    fn test_rate_tracker_burst() {
        let tracker = DefaultRateTracker::new(Duration::from_secs(1));

        // record a lot of events
        for _ in 0..1000 {
            tracker.observe();
        }

        let rate = tracker.rate();
        assert_eq!(rate, 1000.0); // 1000 events in 1 second
    }

    #[test]
    fn test_rate_tracker_threading() {
        let tracker = Arc::new(DefaultRateTracker::new(Duration::from_secs(1)));
        let mut handles = vec![];

        for _ in 0..4 {
            let tracker_clone = Arc::clone(&tracker);
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    tracker_clone.observe();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let rate = tracker.rate();
        assert_eq!(rate, 40.0); // 40 events in 1 second
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeDirection {
    Backwards, // Past (default)
    Forwards,  // Future
}

impl Default for TimeDirection {
    fn default() -> Self {
        TimeDirection::Backwards
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelativeDateTime {
    duration: Duration,
    direction: TimeDirection,
}

impl RelativeDateTime {
    pub fn new(duration: Duration, direction: TimeDirection) -> Self {
        Self {
            duration,
            direction,
        }
    }

    pub fn ago(duration: Duration) -> Self {
        Self::new(duration, TimeDirection::Backwards)
    }

    pub fn from_now(duration: Duration) -> Self {
        let cur = get_time();
        if duration > cur {
            Self::new(duration - cur, TimeDirection::Forwards)
        } else {
            Self::new(cur - duration, TimeDirection::Backwards)
        }
    }
}

impl std::fmt::Display for RelativeDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secs = self.duration.as_secs();

        if secs == 0 {
            return write!(f, "now");
        }

        let (amount, unit) = match secs {
            0 => unreachable!(), // handled above
            1..=59 => (secs, "second"),
            60..=3599 => (secs / 60, "minute"),
            3600..=86399 => (secs / 3600, "hour"),
            86400..=2591999 => (secs / 86400, "day"), // up to 29 days
            2592000..=31535999 => (secs / 2592000, "month"), // 30 days to 364 days
            _ => (secs / 31536000, "year"),           // 365 days+
        };

        let plural = if amount != 1 { "s" } else { "" };

        match self.direction {
            TimeDirection::Forwards => write!(f, "in {} {}{}", amount, unit, plural),
            TimeDirection::Backwards => write!(f, "{} {}{} ago", amount, unit, plural),
        }
    }
}
