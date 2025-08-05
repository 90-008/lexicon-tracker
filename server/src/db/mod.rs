use std::{
    io::Cursor,
    ops::{Bound, Deref, RangeBounds},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};

use atomic_time::AtomicInstant;
use fjall::{Config, Keyspace, Partition, PartitionCreateOptions, Slice};
use ordered_varint::Variable;
use pingora_limits::rate::Rate;
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use smol_str::SmolStr;
use tokio::sync::broadcast;

use crate::{
    db::block::{ReadVariableExt, WriteVariableExt},
    error::{AppError, AppResult},
    jetstream::JetstreamEvent,
    utils::time_now,
};

mod block;

#[derive(Clone, Debug, Default, Archive, Deserialize, Serialize, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct NsidCounts {
    pub count: u128,
    pub deleted_count: u128,
    pub last_seen: u64,
}

#[derive(Debug, Default, Archive, Deserialize, Serialize, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct NsidHit {
    pub deleted: bool,
}

#[derive(Clone)]
pub struct EventRecord {
    pub nsid: SmolStr,
    pub timestamp: u64, // seconds
    pub deleted: bool,
}

impl EventRecord {
    pub fn from_jetstream(event: JetstreamEvent) -> Option<Self> {
        match event {
            JetstreamEvent::Commit {
                time_us, commit, ..
            } => Some(Self {
                nsid: commit.collection.into(),
                timestamp: time_us / 1_000_000,
                deleted: false,
            }),
            JetstreamEvent::Delete {
                time_us, commit, ..
            } => Some(Self {
                nsid: commit.collection.into(),
                timestamp: time_us / 1_000_000,
                deleted: true,
            }),
            _ => None,
        }
    }
}

type ItemDecoder = block::ItemDecoder<Cursor<Slice>, NsidHit>;
type ItemEncoder = block::ItemEncoder<Vec<u8>, NsidHit>;
type Item = block::Item<NsidHit>;

pub struct LexiconHandle {
    tree: Partition,
    buf: Arc<scc::Queue<EventRecord>>,
    buf_len: AtomicUsize,
    last_insert: AtomicInstant,
    eps: Rate,
    block_size: AtomicUsize,
}

impl LexiconHandle {
    fn new(keyspace: &Keyspace, nsid: &str) -> Self {
        let opts = PartitionCreateOptions::default().compression(fjall::CompressionType::Miniz(9));
        Self {
            tree: keyspace.open_partition(nsid, opts).unwrap(),
            buf: Default::default(),
            buf_len: AtomicUsize::new(0),
            last_insert: AtomicInstant::now(),
            eps: Rate::new(Duration::from_secs(5)),
            block_size: AtomicUsize::new(1000),
        }
    }

    fn item_count(&self) -> usize {
        self.buf_len.load(AtomicOrdering::Acquire)
    }

    fn last_insert(&self) -> Instant {
        self.last_insert.load(AtomicOrdering::Acquire)
    }

    fn suggested_block_size(&self) -> usize {
        self.block_size.load(AtomicOrdering::Relaxed)
    }

    fn insert(&self, event: EventRecord) {
        self.buf.push(event);
        self.buf_len.fetch_add(1, AtomicOrdering::Release);
        self.last_insert
            .store(Instant::now(), AtomicOrdering::Release);
        self.eps.observe(&(), 1);
        let rate = self.eps.rate(&()) as usize;
        if rate != 0 {
            self.block_size.store(rate * 60, AtomicOrdering::Relaxed);
        }
    }

    fn sync(&self, max_block_size: usize) -> AppResult<usize> {
        let mut writer = ItemEncoder::new(Vec::with_capacity(
            size_of::<u64>() + self.item_count().min(max_block_size) * size_of::<(u64, NsidHit)>(),
        ));
        let mut start_timestamp = None;
        let mut end_timestamp = None;
        let mut written = 0_usize;
        while let Some(event) = self.buf.pop() {
            let item = Item::new(
                event.timestamp,
                &NsidHit {
                    deleted: event.deleted,
                },
            );
            writer.encode(&item)?;
            if start_timestamp.is_none() {
                start_timestamp = Some(event.timestamp);
            }
            end_timestamp = Some(event.timestamp);
            if written >= max_block_size {
                break;
            }
            written += 1;
        }
        if let (Some(start_timestamp), Some(end_timestamp)) = (start_timestamp, end_timestamp) {
            self.buf_len.store(0, AtomicOrdering::Release);
            let value = writer.finish()?;
            let mut key = Vec::with_capacity(size_of::<u64>() * 2);
            key.write_varint(start_timestamp)?;
            key.write_varint(end_timestamp)?;
            self.tree.insert(key, value)?;
        }
        Ok(written)
    }
}

type BoxedIter<T> = Box<dyn Iterator<Item = T>>;

// counts is nsid -> NsidCounts
// hits is tree per nsid: varint start time + varint end time -> block of hits
pub struct Db {
    inner: Keyspace,
    hits: scc::HashIndex<SmolStr, Arc<LexiconHandle>>,
    counts: Partition,
    event_broadcaster: broadcast::Sender<(SmolStr, NsidCounts)>,
    eps: Rate,
    min_block_size: usize,
    max_block_size: usize,
    max_last_activity: Duration,
}

impl Db {
    pub fn new(path: impl AsRef<Path>) -> AppResult<Self> {
        tracing::info!("opening db...");
        let ks = Config::new(path)
            .cache_size(8 * 1024 * 1024) // from talna
            .open()?;
        Ok(Self {
            hits: Default::default(),
            counts: ks.open_partition(
                "_counts",
                PartitionCreateOptions::default().compression(fjall::CompressionType::None),
            )?,
            inner: ks,
            event_broadcaster: broadcast::channel(1000).0,
            eps: Rate::new(Duration::from_secs(1)),
            min_block_size: 512,
            max_block_size: 500_000,
            max_last_activity: Duration::from_secs(10),
        })
    }

    pub fn sync(&self, all: bool) -> AppResult<()> {
        let _guard = scc::ebr::Guard::new();
        for (nsid, tree) in self.hits.iter(&_guard) {
            let count = tree.item_count();
            let is_max_block_size = count > self.min_block_size.max(tree.suggested_block_size());
            let is_too_old = tree.last_insert().elapsed() > self.max_last_activity;
            if count > 0 && (all || is_max_block_size || is_too_old) {
                loop {
                    let synced = tree.sync(self.max_block_size)?;
                    if synced == 0 {
                        break;
                    }
                    tracing::info!("synced {synced} of {nsid} to db");
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    pub fn eps(&self) -> usize {
        self.eps.rate(&()) as usize
    }

    #[inline(always)]
    pub fn new_listener(&self) -> broadcast::Receiver<(SmolStr, NsidCounts)> {
        self.event_broadcaster.subscribe()
    }

    #[inline(always)]
    fn maybe_run_in_nsid_tree<T>(
        &self,
        nsid: &str,
        f: impl FnOnce(&LexiconHandle) -> T,
    ) -> Option<T> {
        let _guard = scc::ebr::Guard::new();
        let handle = match self.hits.peek(nsid, &_guard) {
            Some(handle) => handle.clone(),
            None => {
                if self.inner.partition_exists(nsid) {
                    let handle = Arc::new(LexiconHandle::new(&self.inner, nsid));
                    let _ = self.hits.insert(SmolStr::new(nsid), handle.clone());
                    handle
                } else {
                    return None;
                }
            }
        };
        Some(f(&handle))
    }

    #[inline(always)]
    fn run_in_nsid_tree<T>(
        &self,
        nsid: SmolStr,
        f: impl FnOnce(&LexiconHandle) -> AppResult<T>,
    ) -> AppResult<T> {
        f(self
            .hits
            .entry(nsid.clone())
            .or_insert_with(move || Arc::new(LexiconHandle::new(&self.inner, &nsid)))
            .get())
    }

    pub fn record_event(&self, e: EventRecord) -> AppResult<()> {
        let EventRecord {
            nsid,
            timestamp,
            deleted,
        } = e.clone();

        // insert event
        self.run_in_nsid_tree(nsid.clone(), move |tree| Ok(tree.insert(e)))?;
        // increment count
        let mut counts = self.get_count(&nsid)?;
        counts.last_seen = timestamp;
        if deleted {
            counts.deleted_count += 1;
        } else {
            counts.count += 1;
        }
        self.insert_count(&nsid, counts.clone())?;
        if self.event_broadcaster.receiver_count() > 0 {
            let _ = self.event_broadcaster.send((SmolStr::new(&nsid), counts));
        }
        self.eps.observe(&(), 1);
        Ok(())
    }

    #[inline(always)]
    fn insert_count(&self, nsid: &str, counts: NsidCounts) -> AppResult<()> {
        self.counts
            .insert(
                nsid,
                unsafe { rkyv::to_bytes::<Error>(&counts).unwrap_unchecked() }.as_slice(),
            )
            .map_err(AppError::from)
    }

    pub fn get_count(&self, nsid: &str) -> AppResult<NsidCounts> {
        let Some(raw) = self.counts.get(nsid)? else {
            return Ok(NsidCounts::default());
        };
        Ok(unsafe { rkyv::from_bytes_unchecked::<_, Error>(&raw).unwrap_unchecked() })
    }

    pub fn get_counts(&self) -> impl Iterator<Item = AppResult<(SmolStr, NsidCounts)>> {
        self.counts.iter().map(|res| {
            res.map_err(AppError::from).map(|(key, val)| {
                (
                    SmolStr::new(unsafe { str::from_utf8_unchecked(&key) }),
                    unsafe { rkyv::from_bytes_unchecked::<_, Error>(&val).unwrap_unchecked() },
                )
            })
        })
    }

    pub fn get_nsids(&self) -> impl Iterator<Item = impl Deref<Target = str> + 'static> {
        self.inner
            .list_partitions()
            .into_iter()
            .filter(|k| k.deref() != "_counts")
    }

    pub fn get_hits_debug(&self, nsid: &str) -> BoxedIter<AppResult<(Slice, Slice)>> {
        self.maybe_run_in_nsid_tree(nsid, |handle| -> BoxedIter<AppResult<(Slice, Slice)>> {
            Box::new(
                handle
                    .tree
                    .iter()
                    .rev()
                    .map(|res| res.map_err(AppError::from)),
            )
        })
        .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    pub fn get_hits(
        &self,
        nsid: &str,
        range: impl RangeBounds<u64> + std::fmt::Debug,
    ) -> BoxedIter<AppResult<Item>> {
        let start = range
            .start_bound()
            .cloned()
            .map(|t| unsafe { t.to_variable_vec().unwrap_unchecked() });
        let end = range
            .end_bound()
            .cloned()
            .map(|t| unsafe { t.to_variable_vec().unwrap_unchecked() });
        let limit = match range.end_bound().cloned() {
            Bound::Included(end) => end,
            Bound::Excluded(end) => end.saturating_sub(1),
            Bound::Unbounded => u64::MAX,
        };

        self.maybe_run_in_nsid_tree(nsid, move |handle| -> BoxedIter<AppResult<Item>> {
            let map_block = move |(key, val)| {
                let mut key_reader = Cursor::new(key);
                let start_timestamp = key_reader.read_varint::<u64>()?;
                let items =
                    ItemDecoder::new(Cursor::new(val), start_timestamp)?.take_while(move |item| {
                        item.as_ref().map_or(true, |item| item.timestamp <= limit)
                    });
                Ok(items)
            };

            Box::new(
                handle
                    .tree
                    .range(TimestampRange { start, end })
                    .map(move |res| res.map_err(AppError::from).and_then(map_block))
                    .flatten()
                    .flatten(),
            )
        })
        .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    pub fn tracking_since(&self) -> AppResult<u64> {
        // HACK: we should actually store when we started tracking but im lazy
        // should be accurate enough
        self.maybe_run_in_nsid_tree("app.bsky.feed.like", |handle| {
            let Some((timestamps_raw, _)) = handle.tree.first_key_value()? else {
                return Ok(0);
            };
            let mut timestamp_reader = Cursor::new(timestamps_raw);
            timestamp_reader
                .read_varint::<u64>()
                .map_err(AppError::from)
        })
        .unwrap_or(Ok(0))
    }
}

type TimestampRepr = Vec<u8>;

struct TimestampRange {
    start: Bound<TimestampRepr>,
    end: Bound<TimestampRepr>,
}

impl RangeBounds<TimestampRepr> for TimestampRange {
    #[inline(always)]
    fn start_bound(&self) -> Bound<&TimestampRepr> {
        self.start.as_ref()
    }

    #[inline(always)]
    fn end_bound(&self) -> Bound<&TimestampRepr> {
        self.end.as_ref()
    }
}
