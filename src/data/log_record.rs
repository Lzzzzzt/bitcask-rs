use std::mem::size_of;
use std::num::{NonZeroU128, NonZeroU64};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bitflags::bitflags;
use bytes::BufMut;
use crc32fast::Hasher;

use crate::errors::{BCResult, Errors};
use crate::file::io::IO;
use crate::utils::{Key, Value};

macro_rules! get {
    ($typ: ty, $data: expr, $index: expr) => {{
        const STEP: usize = std::mem::size_of::<$typ>();
        let _tmp = <$typ>::from_be_bytes(unsafe { *$data[$index..].as_ptr().cast::<[u8; STEP]>() });
        $index += STEP;
        _tmp
    }};
}

/// ## Data Position Index
/// `LogRecordPosition` will describe data store in which position.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct RecordPosition {
    /// file id, distinguish the data store in which file
    pub(crate) fid: u32,
    /// offset, means the position that the data store in data file
    pub(crate) offset: u32,

    pub(crate) size: u32,
}

impl RecordPosition {
    pub(crate) fn new(fid: u32, offset: u32, size: u32) -> Self {
        Self { fid, offset, size }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(size_of::<Self>());

        res.extend_from_slice(&self.fid.to_be_bytes());
        res.extend_from_slice(&self.offset.to_be_bytes());
        res.extend_from_slice(&self.size.to_be_bytes());

        res
    }

    pub(crate) fn decode(data: &[u8]) -> Self {
        let mut index = 0;

        Self {
            fid: get!(u32, data, index),
            offset: get!(u32, data, index),
            #[allow(unused_assignments)]
            size: get!(u32, data, index),
        }
    }
}

bitflags! {
    struct RecordHeaderBits: u8 {
        const DELETED  = 1 << 0;
        const NORMAL   = 1 << 1;
        const COMMITED = 1 << 2;
        const BATCH    = 1 << 3;
        const EXPIRE   = 1 << 4;
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum RecordDataType {
    // the record that be marked deleted
    Deleted,
    // the nomarl data
    Normal,
    // mark commited transacton
    Commited,
}

impl TryFrom<u8> for RecordDataType {
    type Error = Errors;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Deleted),
            1 => Ok(Self::Normal),
            2 => Ok(Self::Commited),
            _ => Err(Errors::UnknownRecordDataType),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecordBatchState {
    Enable(NonZeroU64),
    Disable,
}

impl From<RecordBatchState> for u64 {
    fn from(value: RecordBatchState) -> Self {
        match value {
            RecordBatchState::Enable(u) => u.into(),
            RecordBatchState::Disable => 0,
        }
    }
}

impl From<u64> for RecordBatchState {
    fn from(value: u64) -> Self {
        if value == 0 {
            Self::Disable
        } else {
            Self::Enable(unsafe { NonZeroU64::new_unchecked(value) })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecordExpireState {
    Enable(NonZeroU128),
    Disable,
}

impl From<RecordExpireState> for u128 {
    fn from(value: RecordExpireState) -> Self {
        match value {
            RecordExpireState::Enable(u) => u.into(),
            RecordExpireState::Disable => 0,
        }
    }
}

impl From<u128> for RecordExpireState {
    fn from(value: u128) -> Self {
        if value == 0 {
            Self::Disable
        } else {
            Self::Enable(unsafe { NonZeroU128::new_unchecked(value) })
        }
    }
}

/// the record that will write into the data file
///
/// the reason why being called 'record' is the data in data file is appended, like log record
#[derive(Debug)]
pub struct Record {
    pub(crate) record_type: RecordDataType,
    pub(crate) batch_state: RecordBatchState,
    pub(crate) expire: RecordExpireState,
    pub(crate) key: Key,
    pub(crate) value: Value,
}

impl Record {
    pub fn normal(key: Key, value: Value) -> Self {
        Self {
            key,
            value,
            record_type: RecordDataType::Normal,
            batch_state: RecordBatchState::Disable,
            expire: RecordExpireState::Disable,
        }
    }

    pub fn deleted(key: Key) -> Self {
        Self {
            key,
            value: Default::default(),
            record_type: RecordDataType::Deleted,
            batch_state: RecordBatchState::Disable,
            expire: RecordExpireState::Disable,
        }
    }

    pub fn batch_finished(seq: u64) -> Self {
        Self {
            key: "BF".into(),
            value: Default::default(),
            record_type: RecordDataType::Commited,
            batch_state: seq.into(),
            expire: RecordExpireState::Disable,
        }
    }

    pub fn merge_finished(unmerged: u32) -> Self {
        Self::normal("MF".into(), unmerged.to_be_bytes().into())
    }

    pub fn enable_transaction(mut self, seq_num: u64) -> Self {
        self.batch_state = seq_num.into();
        self
    }

    pub fn disable_transaction(&mut self) {
        self.batch_state = RecordBatchState::Disable
    }

    pub fn expire(mut self, time: Duration) -> BCResult<Self> {
        let expire_time = SystemTime::now()
            .checked_add(time)
            .ok_or(Errors::InvalidExpireTime)?;
        let ts = expire_time.duration_since(UNIX_EPOCH).unwrap().as_micros();

        self.expire = ts.into();
        Ok(self)
    }

    pub async fn encode(&self) -> Vec<u8> {
        let size = self.calculate_encoded_length();
        let mut bytes = Vec::with_capacity(size);

        // total record size
        bytes.put_u64(size as u64);

        let mut meta = RecordHeaderBits::empty();
        // save for meta
        bytes.put_u8(0);

        match self.record_type {
            RecordDataType::Deleted => meta |= RecordHeaderBits::DELETED,
            RecordDataType::Normal => meta |= RecordHeaderBits::NORMAL,
            RecordDataType::Commited => meta |= RecordHeaderBits::COMMITED,
        }

        if RecordBatchState::Disable != self.batch_state {
            meta |= RecordHeaderBits::BATCH;
            bytes.put_u64(self.batch_state.into())
        }

        if RecordExpireState::Disable != self.expire {
            meta |= RecordHeaderBits::EXPIRE;
            bytes.put_u128(self.expire.into());
        }

        unsafe { *bytes.get_unchecked_mut(8) = meta.bits() }

        // then store the key size and value size
        bytes.put_u32(self.key.len() as u32);
        bytes.put_u32(self.value.len() as u32);

        // store the key/value set
        bytes.extend_from_slice(&self.value);
        bytes.extend_from_slice(&self.key);

        // calculate crc then store it
        let crc = calculate_crc_checksum(&bytes);
        bytes.put_u32(crc);

        bytes
    }

    pub async fn crc(&self) -> u32 {
        let encoded = self.encode().await;
        let crc_bytes = encoded.last_chunk::<4>().unwrap();
        u32::from_be_bytes(*crc_bytes)
    }

    pub fn calculate_encoded_length(&self) -> usize {
        let key_len = self.key.len();
        let value_len = self.value.len();

        let mut fixed_length = size_of::<RecordHeaderBits>()
            + size_of::<u32>() * 3
            + key_len
            + value_len
            + size_of::<u64>();

        if matches!(self.batch_state, RecordBatchState::Enable(_)) {
            fixed_length += size_of::<RecordBatchState>()
        }

        if matches!(self.expire, RecordExpireState::Enable(_)) {
            fixed_length += size_of::<RecordExpireState>()
        }

        fixed_length
    }
}

#[inline]
fn calculate_crc_checksum(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

pub struct ReadRecord {
    data: Box<[u8]>,
    key_value_start: u32,
    key_size: u32,
    value_size: u32,
    pub(crate) record_type: RecordDataType,
    pub(crate) batch_state: RecordBatchState,
    pub(crate) expire: RecordExpireState,
}

#[allow(unused)]
impl ReadRecord {
    pub async fn decode<T: IO>(io: &T, offset: u32) -> BCResult<Self> {
        let mut size_bytes: [u8; 8] = [0; 8];
        io.read(&mut size_bytes, offset).await?;
        let size = u64::from_be_bytes(size_bytes);

        if size == 0 {
            return Err(Errors::DataFileEndOfFile);
        }

        let mut data = vec![0u8; size as usize];

        io.read(&mut data, offset).await?;

        Self::decode_vec(data).await
    }

    pub async fn decode_vec(data: Vec<u8>) -> BCResult<Self> {
        let data = data.into_boxed_slice();

        let record_crc = calculate_crc_checksum(&data[..data.len() - 4]);
        let crc = u32::from_be_bytes(*data.last_chunk::<4>().unwrap());

        if record_crc != crc {
            return Err(Errors::InvalidRecordCRC);
        }

        let mut index = size_of::<u64>();

        let meta = RecordHeaderBits::from_bits_truncate(get!(u8, data, index));

        let record_type = if meta.contains(RecordHeaderBits::DELETED) {
            RecordDataType::Deleted
        } else if meta.contains(RecordHeaderBits::COMMITED) {
            RecordDataType::Commited
        } else {
            RecordDataType::Normal
        };

        let batch_state = if meta.contains(RecordHeaderBits::BATCH) {
            get!(u64, data, index).into()
        } else {
            0.into()
        };

        let expire = if meta.contains(RecordHeaderBits::EXPIRE) {
            get!(u128, data, index).into()
        } else {
            0.into()
        };

        // decode key/value size
        let key_size = get!(u32, data, index);
        let value_size = get!(u32, data, index);

        Ok(Self {
            data,
            key_value_start: index as u32,
            key_size,
            value_size,
            record_type,
            batch_state,
            expire,
        })
    }

    pub fn value(&self) -> &[u8] {
        let start = self.key_value_start as usize;
        let end = start + self.value_size as usize;
        &self.data[start..end]
    }

    pub fn key(&self) -> &[u8] {
        let start = (self.key_value_start + self.value_size) as usize;
        let end = start + self.key_size as usize;
        &self.data[start..end]
    }

    pub fn size(&self) -> u32 {
        self.data.len() as u32
    }

    pub fn into_log_record(self) -> Record {
        let value_start = self.key_value_start as usize;
        let value_end = value_start + self.value_size as usize;
        let key_start = value_end;
        let key_end = key_start + self.key_size as usize;
        Record {
            record_type: self.record_type,
            batch_state: self.batch_state,
            expire: self.expire,
            key: self.data[key_start..key_end].to_vec(),
            value: self.data[value_start..value_end].to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn record_encode() {
        // normal record
        let nomarl_record = Record::normal("foo".into(), "bar".into());

        let encoded_normal_record = nomarl_record.encode().await;

        assert!(encoded_normal_record.len() > 5);
        // assert_eq!(3762633406, nomarl_record.crc());

        // value is empty
        let value_is_empty = Record::normal("foo".into(), "".into());

        let encoded_normal_record = value_is_empty.encode().await;

        assert!(encoded_normal_record.len() > 5);
        // assert_eq!(260641321, value_is_empty.crc());

        // type is deleted
        let type_is_deleted = Record::deleted("foo".into());

        let encoded_normal_record = type_is_deleted.encode().await;

        assert!(encoded_normal_record.len() > 5);
        // assert_eq!(2852002205, type_is_deleted.crc());
    }
}
