use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub static CLOCK: std::sync::LazyLock<quanta::Clock> =
    std::sync::LazyLock::new(|| quanta::Clock::new());

/// simple thread-safe rate tracker using time buckets
/// divides time into fixed buckets and rotates through them
#[derive(Debug)]
pub struct RateTracker<const BUCKET_WINDOW: u64> {
    buckets: Vec<AtomicU64>,
    bucket_duration_nanos: u64,
    window_duration: Duration,
    last_bucket_time: AtomicU64,
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

    /// record an event
    pub fn observe(&self) {
        let now = CLOCK.raw();
        self.maybe_advance_buckets(now);

        let bucket_index = self.get_current_bucket_index(now);
        self.buckets[bucket_index].fetch_add(1, Ordering::Relaxed);
    }

    /// get the current rate in events per second
    pub fn rate(&self) -> f64 {
        let now = CLOCK.raw();
        self.maybe_advance_buckets(now);

        let total_events: u64 = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .sum();

        total_events as f64 / self.window_duration.as_secs_f64()
    }

    fn get_current_bucket_index(&self, now: u64) -> usize {
        let elapsed_nanos = CLOCK.delta_as_nanos(self.start_time, now);
        let bucket_number = elapsed_nanos / self.bucket_duration_nanos;
        (bucket_number as usize) % self.buckets.len()
    }

    fn maybe_advance_buckets(&self, now: u64) {
        let elapsed_nanos = CLOCK.delta_as_nanos(self.start_time, now);
        let current_bucket_time =
            (elapsed_nanos / self.bucket_duration_nanos) * self.bucket_duration_nanos;
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
