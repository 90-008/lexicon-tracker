use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    io::Cursor,
    ops::{Bound, Deref, RangeBounds},
    path::Path,
    time::Duration,
    u64,
};

use byteview::StrView;
use fjall::{Keyspace, Partition, PartitionCreateOptions};
use itertools::{Either, Itertools};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rclite::Arc;
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use smol_str::{SmolStr, ToSmolStr};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{
    db::handle::{ItemDecoder, LexiconHandle},
    error::{AppError, AppResult},
    jetstream::JetstreamEvent,
    utils::{CLOCK, RateTracker, ReadVariableExt, varints_unsigned_encoded},
};

mod block;
mod handle;

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

pub struct DbInfo {
    pub nsids: HashMap<SmolStr, Vec<usize>>,
    pub disk_size: u64,
}

pub struct DbConfig {
    pub ks_config: fjall::Config,
    pub min_block_size: usize,
    pub max_block_size: usize,
    pub max_last_activity: Duration,
}

impl DbConfig {
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.ks_config = fjall::Config::new(path);
        self
    }

    pub fn ks(mut self, f: impl FnOnce(fjall::Config) -> fjall::Config) -> Self {
        self.ks_config = f(self.ks_config);
        self
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            ks_config: fjall::Config::default()
                .cache_size(1024 * 1024 * 512)
                .max_write_buffer_size(u64::MAX),
            min_block_size: 1000,
            max_block_size: 250_000,
            max_last_activity: Duration::from_secs(10),
        }
    }
}

// counts is nsid -> NsidCounts
// hits is tree per nsid: varint start time + varint end time -> block of hits
pub struct Db {
    pub cfg: DbConfig,
    pub ks: Keyspace,
    counts: Partition,
    hits: scc::HashIndex<SmolStr, Arc<LexiconHandle>>,
    sync_pool: threadpool::ThreadPool,
    event_broadcaster: broadcast::Sender<(SmolStr, NsidCounts)>,
    eps: RateTracker<100>, // 100 millis buckets
    cancel_token: CancellationToken,
}

impl Db {
    pub fn new(cfg: DbConfig, cancel_token: CancellationToken) -> AppResult<Self> {
        tracing::info!("opening db...");
        let ks = cfg.ks_config.clone().open()?;
        Ok(Self {
            cfg,
            hits: Default::default(),
            sync_pool: threadpool::Builder::new()
                .num_threads(rayon::current_num_threads() * 2)
                .build(),
            counts: ks.open_partition(
                "_counts",
                PartitionCreateOptions::default().compression(fjall::CompressionType::None),
            )?,
            ks,
            event_broadcaster: broadcast::channel(1000).0,
            eps: RateTracker::new(Duration::from_secs(1)),
            cancel_token,
        })
    }

    #[inline(always)]
    pub fn shutting_down(&self) -> impl Future<Output = ()> {
        self.cancel_token.cancelled()
    }

    #[inline(always)]
    pub fn is_shutting_down(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    #[inline(always)]
    pub fn eps(&self) -> usize {
        self.eps.rate() as usize
    }

    #[inline(always)]
    pub fn new_listener(&self) -> broadcast::Receiver<(SmolStr, NsidCounts)> {
        self.event_broadcaster.subscribe()
    }

    pub fn sync(&self, all: bool) -> AppResult<()> {
        let start = CLOCK.now();
        // prepare all the data
        let nsids_len = self.hits.len();
        let mut data = Vec::with_capacity(nsids_len);
        let mut nsids = HashSet::with_capacity(nsids_len);
        let _guard = scc::ebr::Guard::new();
        for (nsid, handle) in self.hits.iter(&_guard) {
            let mut nsid_data = Vec::with_capacity(2);
            let mut total_count = 0;
            let is_too_old = handle.since_last_activity() > self.cfg.max_last_activity;
            // if we disconnect for a long time, we want to sync all of what we
            // have to avoid having many small blocks (even if we run compaction
            // later, it reduces work until we run compaction)
            let block_size = (is_too_old || all)
                .then_some(self.cfg.max_block_size)
                .unwrap_or_else(|| {
                    self.cfg
                        .max_block_size
                        .min(self.cfg.min_block_size.max(handle.suggested_block_size()))
                });
            let count = handle.item_count();
            let data_count = count / block_size;
            if count > 0 && (all || data_count > 0 || is_too_old) {
                for _ in 0..data_count {
                    nsid_data.push((handle.clone(), block_size));
                    total_count += block_size;
                }
                // only sync remainder if we haven't met block size
                let remainder = count % block_size;
                if (all || data_count == 0) && remainder > 0 {
                    nsid_data.push((handle.clone(), remainder));
                    total_count += remainder;
                }
            }
            let _span = handle.span().entered();
            if nsid_data.len() > 0 {
                tracing::info!(
                    {blocks = %nsid_data.len(), count = %total_count},
                    "will encode & sync",
                );
                nsids.insert(nsid.clone());
                data.push(nsid_data);
            }
        }
        drop(_guard);

        // process the blocks
        data.into_par_iter()
            .map(|chunk| {
                chunk
                    .into_iter()
                    .map(|(handle, max_block_size)| {
                        (handle.take_block_items(max_block_size), handle)
                    })
                    .collect::<Vec<_>>()
                    .into_par_iter()
                    .map(|(items, handle)| {
                        let count = items.len();
                        let block = LexiconHandle::encode_block_from_items(items, count)?;
                        AppResult::Ok((block, handle))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .try_for_each(|chunk| {
                let chunk = chunk?;
                for (block, handle) in chunk {
                    self.sync_pool.execute(move || {
                        let _span = handle.span().entered();
                        let written = block.written;
                        match handle.insert_block(block) {
                            Ok(_) => {
                                tracing::info!({count = %written}, "synced")
                            }
                            Err(err) => tracing::error!({ err = %err }, "failed to sync block"),
                        }
                    });
                }
                AppResult::Ok(())
            })?;
        self.sync_pool.join();

        // update snapshots for all (changed) handles
        for nsid in nsids {
            self.hits.peek_with(&nsid, |_, handle| handle.update_tree());
        }

        tracing::info!(time = %start.elapsed().as_secs_f64(), "synced all blocks");

        Ok(())
    }

    pub fn compact(
        &self,
        nsid: impl AsRef<str>,
        max_count: usize,
        range: impl RangeBounds<u64>,
        sort: bool,
    ) -> AppResult<()> {
        let Some(handle) = self.get_handle(nsid) else {
            return Ok(());
        };
        handle.compact(max_count, range, sort)?;
        handle.update_tree();
        Ok(())
    }

    pub fn compact_all(
        &self,
        max_count: usize,
        range: impl RangeBounds<u64> + Clone,
        sort: bool,
    ) -> AppResult<()> {
        for nsid in self.get_nsids() {
            self.compact(nsid, max_count, range.clone(), sort)?;
        }
        Ok(())
    }

    pub fn major_compact(&self) -> AppResult<()> {
        self.compact_all(self.cfg.max_block_size, .., true)?;
        Ok(())
    }

    #[inline(always)]
    fn get_handle(&self, nsid: impl AsRef<str>) -> Option<Arc<LexiconHandle>> {
        let _guard = scc::ebr::Guard::new();
        let handle = match self.hits.peek(nsid.as_ref(), &_guard) {
            Some(handle) => handle.clone(),
            None => {
                if self.ks.partition_exists(nsid.as_ref()) {
                    let handle = Arc::new(LexiconHandle::new(&self.ks, nsid.as_ref()));
                    let _ = self.hits.insert(SmolStr::new(nsid), handle.clone());
                    handle
                } else {
                    return None;
                }
            }
        };
        Some(handle)
    }

    #[inline(always)]
    fn ensure_handle(&self, nsid: &SmolStr) -> impl Deref<Target = Arc<LexiconHandle>> + use<'_> {
        self.hits
            .entry(nsid.clone())
            .or_insert_with(|| Arc::new(LexiconHandle::new(&self.ks, &nsid)))
    }

    pub fn ingest_events(&self, events: impl Iterator<Item = EventRecord>) -> AppResult<()> {
        for (key, chunk) in events.chunk_by(|event| event.nsid.clone()).into_iter() {
            let mut counts = self.get_count(&key)?;
            let mut count = 0;
            self.ensure_handle(&key).queue(chunk.inspect(|e| {
                // increment count
                counts.last_seen = e.timestamp;
                if e.deleted {
                    counts.deleted_count += 1;
                } else {
                    counts.count += 1;
                }
                count += 1;
            }));
            self.eps.observe(count);
            self.insert_count(&key, &counts)?;
            if self.event_broadcaster.receiver_count() > 0 {
                let _ = self.event_broadcaster.send((key, counts));
            }
        }
        Ok(())
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

    pub fn get_nsids(&self) -> impl Iterator<Item = StrView> {
        self.ks
            .list_partitions()
            .into_iter()
            .filter(|k| k.deref() != "_counts")
    }

    pub fn info(&self) -> AppResult<DbInfo> {
        let mut nsids = HashMap::new();
        for nsid in self.get_nsids() {
            let Some(handle) = self.get_handle(&nsid) else {
                continue;
            };
            let block_lens = handle
                .read()
                .iter()
                .rev()
                .try_fold(Vec::new(), |mut acc, item| {
                    let (key, value) = item?;
                    let mut timestamps = Cursor::new(key);
                    let start_timestamp = timestamps.read_varint()?;
                    let decoder = ItemDecoder::new(Cursor::new(value), start_timestamp)?;
                    acc.push(decoder.item_count());
                    AppResult::Ok(acc)
                })?;
            nsids.insert(nsid.to_smolstr(), block_lens);
        }
        Ok(DbInfo {
            nsids,
            disk_size: self.ks.disk_space(),
        })
    }

    pub fn get_hits(
        &self,
        nsid: &str,
        range: impl RangeBounds<u64> + std::fmt::Debug,
        max_items: usize,
    ) -> impl Iterator<Item = AppResult<handle::Item>> {
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

        let Some(handle) = self.get_handle(nsid) else {
            return Either::Right(std::iter::empty());
        };

        // let mut ts = CLOCK.now();
        let map_block = move |(res, current_item_count)| -> AppResult<(Option<_>, usize)> {
            if current_item_count >= max_items {
                return Ok((None, current_item_count));
            }
            let (key, val) = res?;
            let mut key_reader = Cursor::new(key);
            let start_timestamp = key_reader.read_varint::<u64>()?;
            // let end_timestamp = key_reader.read_varint::<u64>()?;
            if start_timestamp < start_limit {
                // tracing::info!(
                //     "stopped at block with timestamps {start_timestamp}..{end_timestamp} because {start_limit} is greater"
                // );
                return Ok((None, current_item_count));
            }
            let decoder = handle::ItemDecoder::new(Cursor::new(val), start_timestamp)?;
            let current_item_count = current_item_count + decoder.item_count();
            // tracing::info!(
            //     "took {}ns to get block with size {}",
            //     ts.elapsed().as_nanos(),
            //     decoder.item_count()
            // );
            // ts = CLOCK.now();
            Ok((
                Some(
                    decoder
                        .take_while(move |item| {
                            item.as_ref().map_or(true, |item| {
                                item.timestamp <= end_limit && item.timestamp >= start_limit
                            })
                        })
                        .map(|res| res.map_err(AppError::from)),
                ),
                current_item_count,
            ))
        };

        let (blocks, _counted) = handle
            .read()
            .range(..end_key)
            .map(|res| res.map_err(AppError::from))
            .rev()
            .fold_while(
                (Vec::with_capacity(20), 0),
                |(mut blocks, current_item_count), res| {
                    use itertools::FoldWhile::*;

                    match map_block((res, current_item_count)) {
                        Ok((Some(block), current_item_count)) => {
                            blocks.push(Ok(block));
                            Continue((blocks, current_item_count))
                        }
                        Ok((None, current_item_count)) => Done((blocks, current_item_count)),
                        Err(err) => {
                            blocks.push(Err(err));
                            Done((blocks, current_item_count))
                        }
                    }
                },
            )
            .into_inner();

        // tracing::info!(
        //     "got blocks with size {}, item count {counted}",
        //     blocks.len()
        // );

        Either::Left(blocks.into_iter().rev().flatten().flatten())
    }

    pub fn tracking_since(&self) -> AppResult<u64> {
        // HACK: we should actually store when we started tracking but im lazy
        // this should be accurate enough
        let Some(handle) = self.get_handle("app.bsky.feed.like") else {
            return Ok(0);
        };
        let Some((timestamps_raw, _)) = handle.read().first_key_value()? else {
            return Ok(0);
        };
        let mut timestamp_reader = Cursor::new(timestamps_raw);
        timestamp_reader
            .read_varint::<u64>()
            .map_err(AppError::from)
    }
}
