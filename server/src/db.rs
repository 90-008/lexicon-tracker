use atproto_jetstream::JetstreamEvent;
use fjall::{Config, Keyspace, Partition, PartitionCreateOptions};
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use smol_str::SmolStr;
use tokio::sync::broadcast;

use crate::error::{AppError, AppResult};

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

pub struct EventRecord {
    nsid: SmolStr,
    timestamp: u64,
    deleted: bool,
}

impl EventRecord {
    pub fn from_jetstream(event: JetstreamEvent) -> Option<Self> {
        match event {
            JetstreamEvent::Commit {
                time_us, commit, ..
            } => Some(Self {
                nsid: commit.collection.into(),
                timestamp: time_us,
                deleted: false,
            }),
            JetstreamEvent::Delete {
                time_us, commit, ..
            } => Some(Self {
                nsid: commit.collection.into(),
                timestamp: time_us,
                deleted: true,
            }),
            _ => None,
        }
    }
}

// counts is nsid -> NsidCounts
// hits is tree per nsid: timestamp -> NsidHit
pub struct Db {
    inner: Keyspace,
    hits: papaya::HashMap<SmolStr, Partition>,
    counts: Partition,
    event_broadcaster: broadcast::Sender<(SmolStr, NsidCounts)>,
}

impl Db {
    pub fn new() -> AppResult<Self> {
        tracing::info!("opening db...");
        let ks = Config::default()
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
        })
    }

    pub fn new_listener(&self) -> broadcast::Receiver<(SmolStr, NsidCounts)> {
        self.event_broadcaster.subscribe()
    }

    #[inline(always)]
    fn run_in_nsid_tree(
        &self,
        nsid: &str,
        f: impl FnOnce(&Partition) -> AppResult<()>,
    ) -> AppResult<()> {
        f(self.hits.pin().get_or_insert_with(SmolStr::new(nsid), || {
            let opts = PartitionCreateOptions::default()
                .compression(fjall::CompressionType::Miniz(9))
                .compaction_strategy(fjall::compaction::Strategy::Fifo(fjall::compaction::Fifo {
                    limit: 5 * 1024 * 1024 * 1024,        // 5 gb
                    ttl_seconds: Some(60 * 60 * 24 * 30), // 30 days
                }));
            self.inner.open_partition(nsid, opts).unwrap()
        }))
    }

    pub fn record_event(&self, e: EventRecord) -> AppResult<()> {
        let EventRecord {
            nsid,
            timestamp,
            deleted,
        } = e;

        self.insert_event(&nsid, timestamp, deleted)?;
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
        Ok(())
    }

    #[inline(always)]
    fn insert_event(&self, nsid: &str, timestamp: u64, deleted: bool) -> AppResult<()> {
        self.run_in_nsid_tree(nsid, |tree| {
            tree.insert(
                timestamp.to_be_bytes(),
                unsafe { rkyv::to_bytes::<Error>(&NsidHit { deleted }).unwrap_unchecked() }
                    .as_slice(),
            )
            .map_err(AppError::from)
        })
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
}
