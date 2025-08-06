use std::{
    io::Cursor,
    ops::{Bound, Deref, RangeBounds},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
    },
    time::Duration,
};

use byteview::ByteView;
use fjall::{Config, Keyspace, Partition, PartitionCreateOptions, Slice};
use itertools::{Either, Itertools};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use smol_str::SmolStr;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{
    error::{AppError, AppResult},
    jetstream::JetstreamEvent,
    utils::{
        CLOCK, DefaultRateTracker, RateTracker, ReadVariableExt, WritableByteView,
        varints_unsigned_encoded,
    },
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
type ItemEncoder = block::ItemEncoder<WritableByteView, NsidHit>;
type Item = block::Item<NsidHit>;

struct Block {
    written: usize,
    key: ByteView,
    data: ByteView,
}

pub struct LexiconHandle {
    tree: Partition,
    nsid: SmolStr,
    buf: Arc<scc::Queue<EventRecord>>,
    // this is stored here since scc::Queue does not have O(1) length
    buf_len: AtomicUsize,   // seqcst
    last_insert: AtomicU64, // relaxed
    eps: DefaultRateTracker,
}

impl LexiconHandle {
    fn new(keyspace: &Keyspace, nsid: &str) -> Self {
        let opts = PartitionCreateOptions::default().compression(fjall::CompressionType::Miniz(9));
        Self {
            tree: keyspace.open_partition(nsid, opts).unwrap(),
            nsid: nsid.into(),
            buf: Default::default(),
            buf_len: AtomicUsize::new(0),
            last_insert: AtomicU64::new(0),
            eps: RateTracker::new(Duration::from_secs(10)),
        }
    }

    fn item_count(&self) -> usize {
        self.buf_len.load(AtomicOrdering::SeqCst)
    }

    fn since_last_activity(&self) -> u64 {
        CLOCK.delta_as_nanos(self.last_insert.load(AtomicOrdering::Relaxed), CLOCK.raw())
    }

    fn suggested_block_size(&self) -> usize {
        self.eps.rate() as usize * 60
    }

    fn insert(&self, event: EventRecord) {
        self.buf.push(event);
        self.buf_len.fetch_add(1, AtomicOrdering::SeqCst);
        self.last_insert.store(CLOCK.raw(), AtomicOrdering::Relaxed);
        self.eps.observe();
    }

    fn encode_block(&self, item_count: usize) -> AppResult<Option<Block>> {
        let mut writer = ItemEncoder::new(
            WritableByteView::with_size(ItemEncoder::encoded_len(item_count)),
            item_count,
        );
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
            if written >= item_count {
                break;
            }
            written += 1;
        }
        if written != item_count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected number of items, invalid data?",
            )
            .into());
        }
        if let (Some(start_timestamp), Some(end_timestamp)) = (start_timestamp, end_timestamp) {
            self.buf_len.store(0, AtomicOrdering::SeqCst);
            let value = writer.finish()?;
            let key = varints_unsigned_encoded([start_timestamp, end_timestamp]);
            return Ok(Some(Block {
                written,
                key,
                data: value.into_inner(),
            }));
        }
        Ok(None)
    }
}

// counts is nsid -> NsidCounts
// hits is tree per nsid: varint start time + varint end time -> block of hits
pub struct Db {
    inner: Keyspace,
    counts: Partition,
    hits: scc::HashIndex<SmolStr, Arc<LexiconHandle>>,
    sync_pool: threadpool::ThreadPool,
    event_broadcaster: broadcast::Sender<(SmolStr, NsidCounts)>,
    eps: RateTracker<100>,
    cancel_token: CancellationToken,
    min_block_size: usize,
    max_block_size: usize,
    max_last_activity: u64,
}

impl Db {
    pub fn new(path: impl AsRef<Path>, cancel_token: CancellationToken) -> AppResult<Self> {
        tracing::info!("opening db...");
        let ks = Config::new(path)
            .cache_size(8 * 1024 * 1024) // from talna
            .open()?;
        Ok(Self {
            hits: Default::default(),
            sync_pool: threadpool::Builder::new()
                .num_threads(rayon::current_num_threads() * 2)
                .build(),
            counts: ks.open_partition(
                "_counts",
                PartitionCreateOptions::default().compression(fjall::CompressionType::None),
            )?,
            inner: ks,
            event_broadcaster: broadcast::channel(1000).0,
            eps: RateTracker::new(Duration::from_secs(1)),
            cancel_token,
            min_block_size: 512,
            max_block_size: 500_000,
            max_last_activity: Duration::from_secs(10).as_nanos() as u64,
        })
    }

    pub fn shutting_down(&self) -> impl Future<Output = ()> {
        self.cancel_token.cancelled()
    }

    pub fn is_shutting_down(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    pub fn sync(&self, all: bool) -> AppResult<()> {
        // prepare all the data
        let mut data = Vec::with_capacity(self.hits.len());
        let _guard = scc::ebr::Guard::new();
        for (_, handle) in self.hits.iter(&_guard) {
            let block_size = self
                .max_block_size
                .min(self.min_block_size.max(handle.suggested_block_size()));
            let count = handle.item_count();
            let data_count = count / block_size;
            let is_too_old = handle.since_last_activity() > self.max_last_activity;
            if count > 0 && (all || data_count > 0 || is_too_old) {
                for i in 0..data_count {
                    data.push((i, handle.clone(), block_size));
                }
                // only sync remainder if we haven't met block size
                let remainder = count % block_size;
                if data_count == 0 && remainder > 0 {
                    data.push((data_count, handle.clone(), remainder));
                }
            }
        }
        drop(_guard);

        // process the blocks
        let mut blocks = Vec::with_capacity(data.len());
        data.into_par_iter()
            .map(|(i, handle, max_block_size)| {
                handle
                    .encode_block(max_block_size)
                    .transpose()
                    .map(|r| r.map(|block| (i, block, handle.clone())))
            })
            .collect_into_vec(&mut blocks);

        // execute into db
        for item in blocks.into_iter() {
            let Some((i, block, handle)) = item.transpose()? else {
                continue;
            };
            self.sync_pool
                .execute(move || match handle.tree.insert(block.key, block.data) {
                    Ok(_) => {
                        tracing::info!("[{i}] synced {} of {} to db", block.written, handle.nsid)
                    }
                    Err(err) => tracing::error!("failed to sync block: {}", err),
                });
        }
        self.sync_pool.join();

        Ok(())
    }

    #[inline(always)]
    pub fn eps(&self) -> usize {
        self.eps.rate() as usize
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
        nsid: &SmolStr,
        f: impl FnOnce(&LexiconHandle) -> AppResult<T>,
    ) -> AppResult<T> {
        f(self
            .hits
            .entry(nsid.clone())
            .or_insert_with(|| Arc::new(LexiconHandle::new(&self.inner, &nsid)))
            .get())
    }

    pub fn ingest_events(&self, events: impl Iterator<Item = EventRecord>) -> AppResult<()> {
        for (key, chunk) in events.chunk_by(|event| event.nsid.clone()).into_iter() {
            let mut counts = self.get_count(&key)?;
            self.run_in_nsid_tree(&key, move |tree| {
                for event in chunk {
                    let EventRecord {
                        timestamp, deleted, ..
                    } = event.clone();

                    tree.insert(event);

                    // increment count
                    counts.last_seen = timestamp;
                    if deleted {
                        counts.deleted_count += 1;
                    } else {
                        counts.count += 1;
                    }

                    self.eps.observe();
                }
                Ok(())
            })?;
            self.insert_count(&key, &counts)?;
            if self.event_broadcaster.receiver_count() > 0 {
                let _ = self.event_broadcaster.send((key, counts));
            }
        }
        Ok(())
    }

    pub fn record_event(&self, e: EventRecord) -> AppResult<()> {
        self.ingest_events(std::iter::once(e))
    }

    #[inline(always)]
    fn insert_count(&self, nsid: &str, counts: &NsidCounts) -> AppResult<()> {
        self.counts
            .insert(
                nsid,
                unsafe { rkyv::to_bytes::<Error>(counts).unwrap_unchecked() }.as_slice(),
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

    pub fn get_hits_debug(&self, nsid: &str) -> impl Iterator<Item = AppResult<(Slice, Slice)>> {
        self.maybe_run_in_nsid_tree(nsid, |handle| {
            Either::Left(
                handle
                    .tree
                    .iter()
                    .rev()
                    .map(|res| res.map_err(AppError::from)),
            )
        })
        .unwrap_or_else(|| Either::Right(std::iter::empty()))
    }

    pub fn get_hits(
        &self,
        nsid: &str,
        range: impl RangeBounds<u64> + std::fmt::Debug,
    ) -> impl Iterator<Item = AppResult<Item>> {
        let start_limit = match range.start_bound().cloned() {
            Bound::Included(start) => start,
            Bound::Excluded(start) => start.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end_limit = match range.end_bound().cloned() {
            Bound::Included(end) => end,
            Bound::Excluded(end) => end.saturating_sub(1),
            Bound::Unbounded => u64::MAX,
        };
        let end_key = varints_unsigned_encoded([end_limit]);

        self.maybe_run_in_nsid_tree(nsid, move |handle| {
            let map_block = move |(key, val)| {
                let mut key_reader = Cursor::new(key);
                let start_timestamp = key_reader.read_varint::<u64>()?;
                if start_timestamp < start_limit {
                    return Ok(None);
                }
                let items = ItemDecoder::new(Cursor::new(val), start_timestamp)?
                    .take_while(move |item| {
                        item.as_ref().map_or(true, |item| {
                            item.timestamp <= end_limit && item.timestamp >= start_limit
                        })
                    })
                    .map(|res| res.map_err(AppError::from));
                Ok(Some(items))
            };

            Either::Left(
                handle
                    .tree
                    .range(..end_key)
                    .rev()
                    .map_while(move |res| {
                        res.map_err(AppError::from).and_then(map_block).transpose()
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .flatten()
                    .flatten(),
            )
        })
        .unwrap_or_else(|| Either::Right(std::iter::empty()))
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
