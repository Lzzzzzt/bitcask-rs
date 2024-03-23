use std::num::NonZeroU64;

use bytes::{Buf, BufMut, BytesMut};
use crc32fast::Hasher;

use crate::errors::{BCResult, Errors};
use crate::file::io::IO;
use crate::utils::{Key, Value};

/// ## Data Position Index
/// `LogRecordPosition` will describe data store in which position.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct RecordPosition {
    /// file id, distinguish the data store in which file
    pub(crate) fid: u32,
    /// offset, means the position that the data store in data file
    pub(crate) offset: u32,
}

impl RecordPosition {
    pub(crate) fn new(fid: u32, offset: u32) -> Self {
        Self { fid, offset }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(8);

        res.extend_from_slice(&self.fid.to_be_bytes());
        res.extend_from_slice(&self.offset.to_be_bytes());

        res
    }

    pub(crate) fn decode(mut data: Vec<u8>) -> Self {
        let offset = data.split_off(4);
        let fid = data;
        Self {
            fid: u32::from_be_bytes([fid[0], fid[1], fid[2], fid[3]]),
            offset: u32::from_be_bytes([offset[0], offset[1], offset[2], offset[3]]),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum RecordType {
    // the record that be marked deleted
    Deleted = 0,
    // the nomarl data
    Normal = 1,
    // mark commited transacton
    Commited = 2,
}

impl TryFrom<u8> for RecordType {
    type Error = Errors;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Deleted),
            1 => Ok(Self::Normal),
            2 => Ok(Self::Commited),
            _ => Err(Errors::UnknownRecordType),
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

/// the record that will write into the data file
///
/// the reason why being called 'record' is the data in data file is appended, like log record
#[derive(Debug)]
pub struct LogRecord {
    pub(crate) record_type: RecordType,
    /// 0 is disable transaction
    pub(crate) batch_state: RecordBatchState,
    pub(crate) key: Key,
    pub(crate) value: Value,
}

impl LogRecord {
    pub fn normal(key: Key, value: Value) -> Self {
        Self {
            key,
            value,
            record_type: RecordType::Normal,
            batch_state: RecordBatchState::Disable,
        }
    }

    pub fn deleted(key: Key) -> Self {
        Self {
            key,
            value: Default::default(),
            record_type: RecordType::Deleted,
            batch_state: RecordBatchState::Disable,
        }
    }

    pub fn batch_finished(seq: u64) -> Self {
        Self {
            key: "BF".into(),
            value: Default::default(),
            record_type: RecordType::Commited,
            batch_state: seq.into(),
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

    pub fn encode(&self) -> Vec<u8> {
        self.encode_and_crc().0
    }

    pub fn crc(&self) -> u32 {
        self.encode_and_crc().1
    }

    fn encode_and_crc(&self) -> (Vec<u8>, u32) {
        let mut bytes = Vec::with_capacity(self.calculate_encoded_length());

        // First bytes store `LogRecordType`
        bytes.put_u8(self.record_type as u8);

        // store the transaction state
        bytes.put_u64(self.batch_state.into());

        // then store the key size and value size
        bytes.put_u32(self.key.len() as u32);
        bytes.put_u32(self.value.len() as u32);

        // store the key/value set
        bytes.extend_from_slice(&self.value);
        bytes.extend_from_slice(&self.key);

        // calculate crc then store it
        let crc = calculate_crc_checksum(&bytes);
        bytes.put_u32(crc);

        (bytes, crc)
    }

    pub fn calculate_encoded_length(&self) -> usize {
        use std::mem::size_of;
        let key_len = self.key.len();
        let value_len = self.value.len();

        size_of::<RecordType>()
            + size_of::<u32>() * 3 // crc, key size, value size
            + key_len
            + value_len
            + size_of::<RecordBatchState>()
    }

    const fn max_record_metadata_size() -> usize {
        std::mem::size_of::<RecordType>()
            + std::mem::size_of::<u32>() * 2
            + std::mem::size_of::<usize>()
    }
}

fn calculate_crc_checksum(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

pub struct ReadLogRecord {
    pub record: LogRecord,
    pub size: u32,
}

impl ReadLogRecord {
    pub fn decode(io: &dyn IO, offset: u32) -> BCResult<Self> {
        // read record metadata(type, key size, value size)
        let mut record_metadata = BytesMut::zeroed(LogRecord::max_record_metadata_size());
        io.read(&mut record_metadata, offset)?;

        // decode record type
        let record_type = record_metadata.get_u8().try_into()?;

        // decode transaction state
        let transaction_state = record_metadata.get_u64();

        // decode key/value size
        let key_size = record_metadata.get_u32() as usize;
        let value_size = record_metadata.get_u32() as usize;

        if key_size == 0 && value_size == 0 {
            return Err(Errors::DataFileEndOfFile);
        }

        // read key/value set and crc
        let mut value_key = vec![0; key_size + value_size + 4];

        // calc actual metadata size
        let actual_metadata_size = LogRecord::max_record_metadata_size();

        io.read(&mut value_key, offset + actual_metadata_size as u32)?;

        // get the crc
        let crc = &value_key[key_size + value_size..];
        let crc = u32::from_be_bytes([crc[0], crc[1], crc[2], crc[3]]);
        value_key.truncate(key_size + value_size);

        // create new key/value just copy the key vec
        let key = value_key.split_off(value_size);
        let value = value_key;

        // construct record
        let record = LogRecord {
            key,
            value,
            record_type,
            batch_state: transaction_state.into(),
        };

        // calc crc and compare
        if record.crc() != crc {
            return Err(Errors::InvalidRecordCRC);
        }

        Ok(Self {
            record,
            size: (actual_metadata_size + key_size + value_size + 4) as u32,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_encode() {
        // normal record
        let nomarl_record = LogRecord::normal("foo".into(), "bar".into());

        let encoded_normal_record = nomarl_record.encode();

        assert!(encoded_normal_record.len() > 5);
        // assert_eq!(3762633406, nomarl_record.crc());

        // value is empty
        let value_is_empty = LogRecord::normal("foo".into(), "".into());

        let encoded_normal_record = value_is_empty.encode();

        assert!(encoded_normal_record.len() > 5);
        // assert_eq!(260641321, value_is_empty.crc());

        // type is deleted
        let type_is_deleted = LogRecord::deleted("foo".into());

        let encoded_normal_record = type_is_deleted.encode();

        assert!(encoded_normal_record.len() > 5);
        // assert_eq!(2852002205, type_is_deleted.crc());
    }
}
