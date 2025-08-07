use std::{
    collections::HashMap,
    fmt::Debug,
    io::Cursor,
    ops::{Bound, Deref, RangeBounds},
    path::{Path, PathBuf},
    time::Duration,
};

use byteview::StrView;
use fjall::{Config, Keyspace, Partition, PartitionCreateOptions};
use itertools::{Either, Itertools};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use rclite::Arc;
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use smol_str::{SmolStr, ToSmolStr};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{
    db::handle::{ItemDecoder, LexiconHandle},
    error::{AppError, AppResult},
    jetstream::JetstreamEvent,
    utils::{RateTracker, ReadVariableExt, varints_unsigned_encoded},
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
    pub max_last_activity: u64,
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
            ks_config: fjall::Config::default(),
            min_block_size: 512,
            max_block_size: 500_000,
            max_last_activity: Duration::from_secs(10).as_nanos() as u64,
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
    eps: RateTracker<100>,
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
        // prepare all the data
        let mut data = Vec::with_capacity(self.hits.len());
        let _guard = scc::ebr::Guard::new();
        for (_, handle) in self.hits.iter(&_guard) {
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
                for i in 0..data_count {
                    nsid_data.push((i, handle.clone(), block_size));
                    total_count += block_size;
                }
                // only sync remainder if we haven't met block size
                let remainder = count % block_size;
                if (all || data_count == 0) && remainder > 0 {
                    nsid_data.push((data_count, handle.clone(), remainder));
                    total_count += remainder;
                }
            }
            tracing::info!(
                "{}: will sync {} blocks ({} count)",
                handle.nsid(),
                nsid_data.len(),
                total_count,
            );
            data.push(nsid_data);
        }
        drop(_guard);

        // process the blocks
        data.into_par_iter()
            .map(|chunk| {
                chunk
                    .into_iter()
                    .map(|(i, handle, max_block_size)| {
                        handle
                            .encode_block(max_block_size)
                            .inspect(|block| {
                                tracing::info!(
                                    "{}: encoded block with {} items",
                                    handle.nsid(),
                                    block.written,
                                )
                            })
                            .map(|block| (i, block, handle))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .try_for_each(|chunk| {
                let chunk = chunk?;
                for (i, block, handle) in chunk {
                    self.sync_pool
                        .execute(move || match handle.insert(block.key, block.data) {
                            Ok(_) => {
                                tracing::info!("{}: [{i}] synced {}", block.written, handle.nsid())
                            }
                            Err(err) => tracing::error!("failed to sync block: {}", err),
                        });
                }
                AppResult::Ok(())
            })?;
        self.sync_pool.join();

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
        handle.compact(max_count, range, sort)
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
        let _guard = scc::ebr::Guard::new();
        for (_, handle) in self.hits.iter(&_guard) {
            handle.deref().major_compact()?;
        }
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
            let block_lens = handle.iter().rev().try_fold(Vec::new(), |mut acc, item| {
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

        let map_block = move |(key, val)| {
            let mut key_reader = Cursor::new(key);
            let start_timestamp = key_reader.read_varint::<u64>()?;
            if start_timestamp < start_limit {
                return Ok(None);
            }
            let items = handle::ItemDecoder::new(Cursor::new(val), start_timestamp)?
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
                .range(..end_key)
                .rev()
                .map_while(move |res| res.map_err(AppError::from).and_then(map_block).transpose())
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .flatten()
                .flatten(),
        )
    }

    pub fn tracking_since(&self) -> AppResult<u64> {
        // HACK: we should actually store when we started tracking but im lazy
        // this should be accurate enough
        let Some(handle) = self.get_handle("app.bsky.feed.like") else {
            return Ok(0);
        };
        let Some((timestamps_raw, _)) = handle.first_key_value()? else {
            return Ok(0);
        };
        let mut timestamp_reader = Cursor::new(timestamps_raw);
        timestamp_reader
            .read_varint::<u64>()
            .map_err(AppError::from)
    }
}
