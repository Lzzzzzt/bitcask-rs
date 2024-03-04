use bytes::{Buf, BufMut, BytesMut};
use crc32fast::Hasher;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::{
    errors::{BCResult, Errors},
    file::io::IO,
};

/// ## Data Position Index
/// `LogRecordPosition` will describe data store in which position.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LogRecordPosition {
    // file id, distinguish the data store in which file
    pub(crate) file_id: u32,
    // offset, means the position that the data store in data file
    pub(crate) offset: usize,
}

impl LogRecordPosition {
    pub(crate) fn new(file_id: u32, offset: usize) -> Self {
        Self { file_id, offset }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum LogRecordType {
    // the record that be marked deleted
    Deleted = 0,
    // the nomarl data
    Normal = 1,
}

impl TryFrom<u8> for LogRecordType {
    type Error = Errors;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Deleted),
            1 => Ok(Self::Normal),
            _ => Err(Errors::UnknownRecordType),
        }
    }
}

/// the record that will write into the data file
///
/// the reason why being called 'record' is the data in data file is appended, like log record
#[derive(Debug)]
pub struct LogRecord {
    pub(crate) record_type: LogRecordType,
    pub(crate) value: Vec<u8>,
    pub(crate) key: Vec<u8>,
}

impl LogRecord {
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

        // then store the key size(max 5 bytes) and value size(max 5 bytes)
        prost::encode_length_delimiter(self.key.len(), &mut bytes).unwrap(); // have allocate enough memory, so just use unwarp
        prost::encode_length_delimiter(self.value.len(), &mut bytes).unwrap();

        // store the key/value set
        bytes.extend_from_slice(&self.key);
        bytes.extend_from_slice(&self.value);

        // calculate crc then store it
        let crc = calculate_crc_checksum(&bytes);
        bytes.put_u32(crc);

        (bytes, crc)
    }

    pub fn calculate_encoded_length(&self) -> usize {
        use std::mem::size_of;
        let key_len = self.key.len();
        let value_len = self.value.len();

        size_of::<LogRecordType>()
            + size_of::<u32>() // crc
            + prost::length_delimiter_len(key_len)
            + prost::length_delimiter_len(value_len)
            + key_len
            + value_len
    }

    fn max_record_metadata_size() -> usize {
        std::mem::size_of::<LogRecordType>() + length_delimiter_len(std::u32::MAX as usize) * 2
    }
}

fn calculate_crc_checksum(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

pub struct ReadLogRecord {
    pub record: LogRecord,
    pub size: usize,
}

impl ReadLogRecord {
    pub fn decode(io: &dyn IO, offset: usize) -> BCResult<Self> {
        // read record metadata(type, key size, value size)
        let mut record_metadata = BytesMut::zeroed(LogRecord::max_record_metadata_size());
        io.read(&mut record_metadata, offset)?;

        // decode record type
        let record_type = record_metadata.get_u8().try_into()?;

        // decode key/value size
        let key_size = decode_length_delimiter(&mut record_metadata).unwrap();
        let value_size = decode_length_delimiter(&mut record_metadata).unwrap();

        if key_size == 0 && value_size == 0 {
            return Err(Errors::DataFileEndOfFile);
        }

        // read key/value set and crc
        let mut key_value = vec![0; key_size + value_size + 4];

        // calc actual metadata size
        let actual_metadata_size =
            length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;

        io.read(&mut key_value, offset + actual_metadata_size)?;

        // get the crc
        let crc = &key_value[key_size + value_size..];
        let crc = u32::from_be_bytes([crc[0], crc[1], crc[2], crc[3]]);
        key_value.truncate(key_size + value_size);

        // create new key/value just copy the value vec
        let value = key_value.split_off(key_size);
        let key = key_value;

        // construct record
        let record = LogRecord {
            key,
            value,
            record_type,
        };

        // calc crc and compare
        if record.crc() != crc {
            return Err(Errors::InvalidRecordCRC);
        }

        Ok(ReadLogRecord {
            record,
            size: actual_metadata_size + key_size + value_size + 4,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_encode() {
        // normal record
        let nomarl_record = LogRecord {
            key: "foo".into(),
            value: "bar".into(),
            record_type: LogRecordType::Normal,
        };

        let encoded_normal_record = nomarl_record.encode();

        assert!(encoded_normal_record.len() > 5);
        assert_eq!(1629594533, nomarl_record.crc());

        // value is empty
        let value_is_empty = LogRecord {
            key: "foo".into(),
            value: Default::default(),
            record_type: LogRecordType::Normal,
        };

        let encoded_normal_record = value_is_empty.encode();

        assert!(encoded_normal_record.len() > 5);
        assert_eq!(1309455589, value_is_empty.crc());

        // type is deleted
        let type_is_deleted = LogRecord {
            key: "foo".into(),
            value: "bar".into(),
            record_type: LogRecordType::Deleted,
        };

        let encoded_normal_record = type_is_deleted.encode();

        assert!(encoded_normal_record.len() > 5);
        assert_eq!(1985656806, type_is_deleted.crc());
    }
}
