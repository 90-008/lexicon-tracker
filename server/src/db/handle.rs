use std::{
    fmt::Debug,
    io::Cursor,
    ops::{Bound, Deref, RangeBounds},
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
    time::Duration,
};

use byteview::ByteView;
use fjall::{Keyspace, Partition, PartitionCreateOptions, Slice};
use itertools::Itertools;
use parking_lot::Mutex;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rclite::Arc;
use smol_str::SmolStr;

use crate::{
    db::{EventRecord, NsidHit, block},
    error::AppResult,
    utils::{CLOCK, DefaultRateTracker, RateTracker, ReadVariableExt, varints_unsigned_encoded},
};

pub type ItemDecoder = block::ItemDecoder<Cursor<Slice>, NsidHit>;
pub type ItemEncoder = block::ItemEncoder<Vec<u8>, NsidHit>;
pub type Item = block::Item<NsidHit>;

pub struct Block {
    pub written: usize,
    pub key: ByteView,
    pub data: Vec<u8>,
}

pub struct LexiconHandle {
    tree: Partition,
    nsid: SmolStr,
    buf: Arc<Mutex<Vec<EventRecord>>>,
    last_insert: AtomicU64, // relaxed
    eps: DefaultRateTracker,
}

impl Debug for LexiconHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LexiconHandle")
            .field("nsid", self.nsid())
            .finish()
    }
}

impl Deref for LexiconHandle {
    type Target = Partition;

    fn deref(&self) -> &Self::Target {
        &self.tree
    }
}

impl LexiconHandle {
    pub fn new(keyspace: &Keyspace, nsid: &str) -> Self {
        let opts = PartitionCreateOptions::default()
            .block_size(1024 * 16)
            .compression(fjall::CompressionType::Miniz(9));
        Self {
            tree: keyspace.open_partition(nsid, opts).unwrap(),
            nsid: nsid.into(),
            buf: Default::default(),
            last_insert: AtomicU64::new(0),
            eps: RateTracker::new(Duration::from_secs(10)),
        }
    }

    pub fn span(&self) -> tracing::Span {
        tracing::info_span!("handle", nsid = %self.nsid)
    }

    pub fn nsid(&self) -> &SmolStr {
        &self.nsid
    }

    pub fn item_count(&self) -> usize {
        self.buf.lock().len()
    }

    pub fn since_last_activity(&self) -> Duration {
        Duration::from_nanos(
            CLOCK.delta_as_nanos(self.last_insert.load(AtomicOrdering::Relaxed), CLOCK.raw()),
        )
    }

    pub fn suggested_block_size(&self) -> usize {
        self.eps.rate() as usize * 60
    }

    pub fn queue(&self, events: impl IntoIterator<Item = EventRecord>) {
        let mut count = 0;
        self.buf.lock().extend(events.into_iter().inspect(|_| {
            count += 1;
        }));
        self.last_insert.store(CLOCK.raw(), AtomicOrdering::Relaxed);
        self.eps.observe(count);
    }

    pub fn compact(
        &self,
        compact_to: usize,
        range: impl RangeBounds<u64>,
        sort: bool,
    ) -> AppResult<()> {
        let _span = self.span().entered();

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

        let start_key = varints_unsigned_encoded([start_limit]);
        let end_key = varints_unsigned_encoded([end_limit]);

        let blocks_to_compact = self
            .tree
            .range(start_key..end_key)
            .collect::<Result<Vec<_>, _>>()?;
        if blocks_to_compact.len() < 2 {
            return Ok(());
        }

        let start_blocks_size = blocks_to_compact.len();
        let keys_to_delete = blocks_to_compact.iter().map(|(key, _)| key);
        let mut all_items =
            blocks_to_compact
                .iter()
                .try_fold(Vec::new(), |mut acc, (key, value)| {
                    let mut timestamps = Cursor::new(key);
                    let start_timestamp = timestamps.read_varint()?;
                    let decoder = block::ItemDecoder::new(Cursor::new(value), start_timestamp)?;
                    let mut items = decoder.collect::<Result<Vec<_>, _>>()?;
                    acc.append(&mut items);
                    AppResult::Ok(acc)
                })?;

        if sort {
            all_items.sort_unstable_by_key(|e| std::cmp::Reverse(e.timestamp));
        }

        let new_blocks = all_items
            .into_iter()
            .chunks(compact_to)
            .into_iter()
            .map(|chunk| chunk.collect_vec())
            .collect_vec()
            .into_par_iter()
            .map(|chunk| {
                let count = chunk.len();
                Self::encode_block_from_items(chunk, count)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let end_blocks_size = new_blocks.len();

        for key in keys_to_delete {
            self.tree.remove(key.clone())?;
        }
        for block in new_blocks {
            self.tree.insert(block.key, block.data)?;
        }

        let reduction =
            ((start_blocks_size - end_blocks_size) as f64 / start_blocks_size as f64) * 100.0;
        tracing::info!(
            {
                start = start_blocks_size,
                end = end_blocks_size,
            },
            "blocks compacted {reduction:.2}%",
        );

        Ok(())
    }

    pub fn encode_block_from_items(
        items: impl IntoIterator<Item = Item>,
        count: usize,
    ) -> AppResult<Block> {
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no items requested",
            )
            .into());
        }
        let mut writer =
            ItemEncoder::new(Vec::with_capacity(ItemEncoder::encoded_len(count)), count);
        let mut start_timestamp = None;
        let mut end_timestamp = None;
        let mut written = 0_usize;
        for item in items.into_iter().take(count) {
            writer.encode(&item)?;
            if start_timestamp.is_none() {
                start_timestamp = Some(item.timestamp);
            }
            end_timestamp = Some(item.timestamp);
            written += 1;
        }
        if written != count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected number of items, invalid data?",
            )
            .into());
        }
        if let (Some(start_timestamp), Some(end_timestamp)) = (start_timestamp, end_timestamp) {
            let value = writer.finish()?;
            let key = varints_unsigned_encoded([start_timestamp, end_timestamp]);
            return Ok(Block {
                written,
                key,
                data: value,
            });
        }
        Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "no items are in queue").into())
    }

    pub fn take_block_items(&self, item_count: usize) -> Vec<Item> {
        let mut buf = self.buf.lock();
        let end = item_count.min(buf.len());
        buf.drain(..end)
            .map(|event| {
                Item::new(
                    event.timestamp,
                    &NsidHit {
                        deleted: event.deleted,
                    },
                )
            })
            .collect()
    }
}
