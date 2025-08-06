use ordered_varint::Variable;
use rkyv::{
    Archive, Serialize, api::high::HighSerializer, rancor, ser::allocator::ArenaHandle,
    util::AlignedVec,
};
use std::{
    io::{self, Read, Write},
    marker::PhantomData,
};

pub struct Item<T> {
    pub timestamp: u64,
    data: AlignedVec,
    phantom: PhantomData<T>,
}

impl<T: Archive> Item<T> {
    pub fn access(&self) -> &T::Archived {
        unsafe { rkyv::access_unchecked::<T::Archived>(&self.data) }
    }
}

impl<T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>> Item<T> {
    pub fn new(timestamp: u64, data: &T) -> Self {
        Item {
            timestamp,
            data: unsafe { rkyv::to_bytes(data).unwrap_unchecked() },
            phantom: PhantomData,
        }
    }
}

pub struct ItemEncoder<W: Write, T> {
    writer: W,
    prev_timestamp: u64,
    prev_delta: i64,
    _item: PhantomData<T>,
}

impl<W: Write, T> ItemEncoder<W, T> {
    pub fn new(writer: W) -> Self {
        ItemEncoder {
            writer,
            prev_timestamp: 0,
            prev_delta: 0,
            _item: PhantomData,
        }
    }

    pub fn encode(&mut self, item: &Item<T>) -> io::Result<()> {
        if self.prev_timestamp == 0 {
            // self.writer.write_varint(item.timestamp)?;
            self.prev_timestamp = item.timestamp;
            self.write_data(&item.data)?;
            return Ok(());
        }

        let delta = (item.timestamp as i128 - self.prev_timestamp as i128) as i64;

        self.writer.write_varint(delta - self.prev_delta)?;
        self.prev_timestamp = item.timestamp;
        self.prev_delta = delta;

        self.write_data(&item.data)?;

        Ok(())
    }

    fn write_data(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_varint(data.len())?;
        self.writer.write_all(data)?;
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<W> {
        self.writer.flush()?;
        Ok(self.writer)
    }
}

pub struct ItemDecoder<R, T> {
    reader: R,
    current_timestamp: u64,
    current_delta: i64,
    first_item: bool,
    _item: PhantomData<T>,
}

impl<R: Read, T: Archive> ItemDecoder<R, T> {
    pub fn new(reader: R, start_timestamp: u64) -> io::Result<Self> {
        Ok(ItemDecoder {
            reader,
            current_timestamp: start_timestamp,
            current_delta: 0,
            first_item: true,
            _item: PhantomData,
        })
    }

    pub fn decode(&mut self) -> io::Result<Option<Item<T>>> {
        if self.first_item {
            // read the first timestamp
            // let timestamp = match self.reader.read_varint::<u64>() {
            //     Ok(timestamp) => timestamp,
            //     Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            //     Err(e) => return Err(e.into()),
            // };
            // self.current_timestamp = timestamp;

            let Some(data_raw) = self.read_item()? else {
                return Ok(None);
            };
            self.first_item = false;
            return Ok(Some(Item {
                timestamp: self.current_timestamp,
                data: data_raw,
                phantom: PhantomData,
            }));
        }

        let Some(_delta) = self.read_timestamp()? else {
            return Ok(None);
        };

        // read data
        let data_raw = match self.read_item()? {
            Some(data_raw) => data_raw,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "expected data after delta",
                )
                .into());
            }
        };

        Ok(Some(Item {
            timestamp: self.current_timestamp,
            data: data_raw,
            phantom: PhantomData,
        }))
    }

    // [10, 11, 12, 14] -> [1, 1, 2] -> [0, 1]
    fn read_timestamp(&mut self) -> io::Result<Option<u64>> {
        let delta = match self.reader.read_varint::<i64>() {
            Ok(delta) => delta,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        self.current_delta += delta;
        self.current_timestamp =
            (self.current_timestamp as i128 + self.current_delta as i128) as u64;
        Ok(Some(self.current_timestamp))
    }

    fn read_item(&mut self) -> io::Result<Option<AlignedVec>> {
        let data_len = match self.reader.read_varint::<usize>() {
            Ok(data_len) => data_len,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut data_raw = AlignedVec::with_capacity(data_len);
        for _ in 0..data_len {
            data_raw.push(0);
        }
        self.reader.read_exact(data_raw.as_mut_slice())?;
        Ok(Some(data_raw))
    }
}

impl<R: Read, T: Archive> Iterator for ItemDecoder<R, T> {
    type Item = io::Result<Item<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.decode().transpose()
    }
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

#[cfg(test)]
mod test {
    use super::*;
    use rkyv::{Archive, Deserialize, Serialize};
    use std::io::Cursor;

    #[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
    #[rkyv(compare(PartialEq))]
    struct TestData {
        id: u32,
        value: String,
    }

    #[test]
    fn test_encoder_decoder_single_item() {
        let data = TestData {
            id: 123,
            value: "test".to_string(),
        };

        let item = Item::new(1000, &data);

        // encode
        let mut buffer = Vec::new();
        let mut encoder = ItemEncoder::new(&mut buffer);
        encoder.encode(&item).unwrap();
        encoder.finish().unwrap();

        // decode
        let cursor = Cursor::new(buffer);
        let mut decoder = ItemDecoder::<_, TestData>::new(cursor, 1000).unwrap();

        let decoded_item = decoder.decode().unwrap().unwrap();
        assert_eq!(decoded_item.timestamp, 1000);

        let decoded_data = decoded_item.access();
        assert_eq!(decoded_data.id, 123);
        assert_eq!(decoded_data.value.as_str(), "test");
    }

    #[test]
    fn test_encoder_decoder_multiple_items() {
        let items = vec![
            Item::new(
                1000,
                &TestData {
                    id: 1,
                    value: "first".to_string(),
                },
            ),
            Item::new(
                1010,
                &TestData {
                    id: 2,
                    value: "second".to_string(),
                },
            ),
            Item::new(
                1015,
                &TestData {
                    id: 3,
                    value: "third".to_string(),
                },
            ),
            Item::new(
                1025,
                &TestData {
                    id: 4,
                    value: "fourth".to_string(),
                },
            ),
        ];

        // encode
        let mut buffer = Vec::new();
        let mut encoder = ItemEncoder::new(&mut buffer);

        for item in &items {
            encoder.encode(item).unwrap();
        }
        encoder.finish().unwrap();

        // decode
        let cursor = Cursor::new(buffer);
        let mut decoder = ItemDecoder::<_, TestData>::new(cursor, 1000).unwrap();

        let mut decoded_items = Vec::new();
        while let Some(item) = decoder.decode().unwrap() {
            decoded_items.push(item);
        }

        assert_eq!(decoded_items.len(), 4);

        for (original, decoded) in items.iter().zip(decoded_items.iter()) {
            assert_eq!(original.timestamp, decoded.timestamp);
            assert_eq!(original.access().id, decoded.access().id);
            assert_eq!(
                original.access().value.as_str(),
                decoded.access().value.as_str()
            );
        }
    }

    #[test]
    fn test_encoder_decoder_with_iterator() {
        let items = vec![
            Item::new(
                2000,
                &TestData {
                    id: 10,
                    value: "a".to_string(),
                },
            ),
            Item::new(
                2005,
                &TestData {
                    id: 20,
                    value: "b".to_string(),
                },
            ),
            Item::new(
                2012,
                &TestData {
                    id: 30,
                    value: "c".to_string(),
                },
            ),
        ];

        // encode
        let mut buffer = Vec::new();
        let mut encoder = ItemEncoder::new(&mut buffer);

        for item in &items {
            encoder.encode(item).unwrap();
        }
        encoder.finish().unwrap();

        // decode
        let cursor = Cursor::new(buffer);
        let decoder = ItemDecoder::<_, TestData>::new(cursor, 2000).unwrap();

        let decoded_items: Result<Vec<_>, _> = decoder.collect();
        let decoded_items = decoded_items.unwrap();

        assert_eq!(decoded_items.len(), 3);
        assert_eq!(decoded_items[0].timestamp, 2000);
        assert_eq!(decoded_items[1].timestamp, 2005);
        assert_eq!(decoded_items[2].timestamp, 2012);

        assert_eq!(decoded_items[0].access().id, 10);
        assert_eq!(decoded_items[1].access().id, 20);
        assert_eq!(decoded_items[2].access().id, 30);
    }

    #[test]
    fn test_delta_compression() {
        let items = vec![
            Item::new(
                1000,
                &TestData {
                    id: 1,
                    value: "a".to_string(),
                },
            ),
            Item::new(
                1010,
                &TestData {
                    id: 2,
                    value: "b".to_string(),
                },
            ), // delta = 10
            Item::new(
                1020,
                &TestData {
                    id: 3,
                    value: "c".to_string(),
                },
            ), // delta = 10, delta-of-delta = 0
            Item::new(
                1025,
                &TestData {
                    id: 4,
                    value: "d".to_string(),
                },
            ), // delta = 5, delta-of-delta = -5
        ];

        let mut buffer = Vec::new();
        let mut encoder = ItemEncoder::new(&mut buffer);

        for item in &items {
            encoder.encode(item).unwrap();
        }
        encoder.finish().unwrap();

        // decode and verify
        let cursor = Cursor::new(buffer);
        let decoder = ItemDecoder::<_, TestData>::new(cursor, 1000).unwrap();

        let decoded_items: Result<Vec<_>, _> = decoder.collect();
        let decoded_items = decoded_items.unwrap();

        for (original, decoded) in items.iter().zip(decoded_items.iter()) {
            assert_eq!(original.timestamp, decoded.timestamp);
            assert_eq!(original.access().id, decoded.access().id);
        }
    }

    #[test]
    fn test_empty_decode() {
        let buffer = Vec::new();
        let cursor = Cursor::new(buffer);
        let mut decoder = ItemDecoder::<_, TestData>::new(cursor, 1000).unwrap();

        let result = decoder.decode().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_backwards_timestamp() {
        let items = vec![
            Item::new(
                1000,
                &TestData {
                    id: 1,
                    value: "first".to_string(),
                },
            ),
            Item::new(
                900,
                &TestData {
                    id: 2,
                    value: "second".to_string(),
                },
            ),
        ];

        let mut buffer = Vec::new();
        let mut encoder = ItemEncoder::new(&mut buffer);

        for item in &items {
            encoder.encode(item).unwrap();
        }
        encoder.finish().unwrap();

        let cursor = Cursor::new(buffer);
        let decoder = ItemDecoder::<_, TestData>::new(cursor, 1000).unwrap();

        let decoded_items: Result<Vec<_>, _> = decoder.collect();
        let decoded_items = decoded_items.unwrap();

        assert_eq!(decoded_items.len(), 2);
        assert_eq!(decoded_items[0].timestamp, 1000);
        assert_eq!(decoded_items[1].timestamp, 900);
    }

    #[test]
    fn test_different_data_sizes() {
        let small_data = TestData {
            id: 1,
            value: "x".to_string(),
        };
        let large_data = TestData {
            id: 2,
            value: "a".repeat(1000),
        };

        let items = vec![Item::new(1000, &small_data), Item::new(1001, &large_data)];

        let mut buffer = Vec::new();
        let mut encoder = ItemEncoder::new(&mut buffer);

        for item in &items {
            encoder.encode(item).unwrap();
        }
        encoder.finish().unwrap();

        let cursor = Cursor::new(buffer);
        let decoder = ItemDecoder::<_, TestData>::new(cursor, 1000).unwrap();

        let decoded_items: Result<Vec<_>, _> = decoder.collect();
        let decoded_items = decoded_items.unwrap();

        assert_eq!(decoded_items.len(), 2);
        assert_eq!(decoded_items[0].access().value.as_str(), "x");
        assert_eq!(decoded_items[1].access().value.len(), 1000);
        assert_eq!(decoded_items[1].access().value.as_str(), "a".repeat(1000));
    }
}
