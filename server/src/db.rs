use fjall::{Config, Keyspace, Partition, PartitionCreateOptions};
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use smol_str::SmolStr;

use crate::error::{AppError, AppResult};

#[derive(Debug, Default, Archive, Deserialize, Serialize, PartialEq)]
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

pub struct Db {
    inner: Keyspace,
    hits: papaya::HashMap<SmolStr, Partition>,
    counts: Partition,
}

impl Db {
    pub fn new() -> AppResult<Self> {
        tracing::info!("opening db...");
        let inner = Keyspace::open(Config::default())?;
        Ok(Self {
            hits: Default::default(),
            counts: inner.open_partition("_counts", PartitionCreateOptions::default())?,
            inner,
        })
    }

    #[inline(always)]
    fn run_in_nsid_tree(
        &self,
        nsid: &str,
        f: impl FnOnce(&Partition) -> AppResult<()>,
    ) -> AppResult<()> {
        f(self.hits.pin().get_or_insert_with(SmolStr::new(nsid), || {
            self.inner
                .open_partition(nsid, PartitionCreateOptions::default())
                .unwrap()
        }))
    }

    pub fn record_event(&self, nsid: &str, timestamp: u64, deleted: bool) -> AppResult<()> {
        self.insert_event(nsid, timestamp, deleted)?;
        // increment count
        let mut counts = self.get_count(nsid)?;
        counts.last_seen = timestamp;
        if deleted {
            counts.deleted_count += 1;
        } else {
            counts.count += 1;
        }
        self.insert_count(nsid, counts)?;
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
