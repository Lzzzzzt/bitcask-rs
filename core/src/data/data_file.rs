use std::path::Path;

use crate::config::IOType;
use crate::errors::BCResult;
use crate::file::io::{create_io_manager, IO};

use crate::consts::*;
use crate::utils::data_file_name;

use super::log_record::{ReadRecord, Record};

#[cfg(feature = "compression")]
use {
    crate::errors::Errors,
    brotli::enc::BrotliEncoderParams,
    std::{io::Cursor, mem::size_of},
};
#[cfg(feature = "compression")]
lazy_static::lazy_static! {
    static ref COMPRESSION_PARAMS: BrotliEncoderParams = BrotliEncoderParams::default();
}

pub struct DataFile {
    pub(crate) id: u32,
    pub(crate) write_offset: u32,
    io: Box<dyn IO>,
}

impl DataFile {
    pub fn new<P: AsRef<Path>>(directory: P, id: u32) -> BCResult<Self> {
        // construct complete filename
        let filename = data_file_name(directory, id);

        Ok(Self {
            id,
            write_offset: 0,
            io: create_io_manager(filename, IOType::System)?,
        })
    }

    pub fn new_mapped<P: AsRef<Path>>(directory: P, id: u32) -> BCResult<Self> {
        let filename = data_file_name(directory, id);

        Ok(Self {
            id,
            write_offset: 0,
            io: create_io_manager(filename, IOType::MemoryMap)?,
        })
    }

    pub fn hint_file<P: AsRef<Path>>(path: P) -> BCResult<Self> {
        let filename = path.as_ref().join(DB_HINT_FILE);

        Ok(Self {
            id: 0,
            write_offset: 0,
            io: create_io_manager(filename, IOType::System)?,
        })
    }

    pub fn merge_finish_file<P: AsRef<Path>>(path: P) -> BCResult<Self> {
        let filename = path.as_ref().join(DB_MERGE_FIN_FILE);

        Ok(Self {
            id: 0,
            write_offset: 0,
            io: create_io_manager(filename, IOType::System)?,
        })
    }

    pub fn sync(&self) -> BCResult<()> {
        self.io.sync()
    }

    pub fn reset_io_type<P: AsRef<Path>>(&mut self, directory: P) -> BCResult<()> {
        self.io = create_io_manager(data_file_name(directory, self.id), IOType::System)?;

        Ok(())
    }

    pub fn padding(&mut self, max: u32) -> BCResult<()> {
        let len = max - self.write_offset;
        let buf = vec![0; len as usize];
        self.io.write(&buf, self.write_offset)?;
        Ok(())
    }
}

#[cfg(not(feature = "compression"))]
impl DataFile {
    pub fn write_record(&mut self, record: &Record) -> BCResult<u32> {
        let encoded = record.encode();
        let write_size = self.io.write(&encoded, self.write_offset)?;
        self.write_offset += write_size;
        Ok(write_size)
    }

    /// read `ReadLogRecord` from data file by offset
    pub fn read_record(&self, offset: u32) -> BCResult<ReadRecord> {
        ReadRecord::decode(self.io.as_ref(), offset)
    }

    pub fn read_record_with_size(&self, offset: u32, size: u32) -> BCResult<ReadRecord> {
        let mut data = vec![0; size as usize];
        self.io.read(&mut data, offset)?;
        ReadRecord::decode_vec(data)
    }
}

#[cfg(feature = "compression")]
impl DataFile {
    pub fn write_record(&mut self, record: &Record) -> BCResult<u32> {
        let encoded = record.encode();

        let mut to_compress = Cursor::new(&encoded[size_of::<u64>()..]);
        let mut compressed: Vec<_> = vec![];

        brotli::BrotliCompress(&mut to_compress, &mut compressed, &COMPRESSION_PARAMS)
            .map_err(Errors::CompressionFailed)?;

        let mut compressed_size = (compressed.len() + 8).to_be_bytes().to_vec();

        compressed_size.extend_from_slice(&compressed);

        let write_size = self.io.write(&compressed_size, self.write_offset)?;

        self.write_offset += write_size;
        Ok(write_size)
    }

    pub fn read_record(&self, offset: u32) -> BCResult<ReadRecord> {
        ReadRecord::decode(self.io.as_ref(), offset)
    }

    pub fn read_record_with_size(&self, offset: u32, size: u32) -> BCResult<ReadRecord> {
        let mut data = vec![0; size as usize];
        self.io.read(&mut data, offset)?;

        let mut data_without_size = Cursor::new(&data[8..]);

        let mut decompressed = vec![];

        brotli::BrotliDecompress(&mut data_without_size, &mut decompressed)
            .map_err(Errors::CompressionFailed)?;

        let mut length = (decompressed.len() + 8).to_be_bytes().to_vec();

        length.extend_from_slice(&decompressed);

        ReadRecord::decode_vec(length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();

        let data_file1 = DataFile::new(temp_dir.path(), 0)?;
        assert_eq!(data_file1.id, 0);
        assert_eq!(data_file1.write_offset, 0);

        let data_file2 = DataFile::new(temp_dir.path(), 0)?;
        assert_eq!(data_file2.id, 0);
        assert_eq!(data_file2.write_offset, 0);

        let data_file3 = DataFile::new(temp_dir.path(), 666)?;
        assert_eq!(data_file3.id, 666);
        assert_eq!(data_file3.write_offset, 0);

        // test new hint file
        let hint_file = DataFile::hint_file(temp_dir.path())?;
        assert_eq!(hint_file.id, 0);

        // test new merge finish file
        let merge_finish_file = DataFile::merge_finish_file(temp_dir.path())?;
        assert_eq!(merge_finish_file.id, 0);

        // test new mapped file
        let temp_dir = tempfile::tempdir().unwrap();
        // create system file for mmap
        std::fs::File::create(data_file_name(temp_dir.path(), 0)).unwrap();

        let data_file = DataFile::new_mapped(temp_dir.path(), 0)?;
        assert_eq!(data_file.id, 0);
        assert_eq!(data_file.write_offset, 0);

        Ok(())
    }

    #[test]
    fn write() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_file = DataFile::new(temp_dir.path(), 0)?;
        assert_eq!(data_file.id, 0);

        let record = Record::normal("foo".into(), "bar".into());

        let encoded_record = record.encode();
        let write_size = data_file.write_record(&record)?;
        assert_eq!(encoded_record.len() as u32, write_size);

        let record = Record::normal("foo".into(), "".into());

        let encoded_record = record.encode();
        let write_size = data_file.write_record(&record)?;
        assert_eq!(encoded_record.len() as u32, write_size);

        let record = Record::deleted("foo".into());

        let encoded_record = record.encode();
        let write_size = data_file.write_record(&record)?;
        assert_eq!(encoded_record.len() as u32, write_size);

        Ok(())
    }

    #[test]
    fn read() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();

        let mut data_file = DataFile::new(temp_dir.path(), 0)?;
        assert_eq!(data_file.id, 0);

        // normal record
        let record = Record::normal("foo".into(), "baraaa".into());

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(0)?;

        let mut size = read_record.size();

        assert_eq!(record.key, read_record.key());
        assert_eq!(record.value, read_record.value());
        assert_eq!(record.record_type, read_record.record_type);

        // value is empty
        let record = Record::normal("foo".into(), Default::default());

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(size)?;
        size += read_record.size();

        assert_eq!(record.key, read_record.key());
        assert_eq!(record.value, read_record.value());
        assert_eq!(record.record_type, read_record.record_type);

        // type is deleted
        let record = Record::deleted("foo".into());

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(size)?;

        assert_eq!(record.key, read_record.key());
        assert_eq!(record.value, read_record.value());
        assert_eq!(record.record_type, read_record.record_type);

        Ok(())
    }

    #[test]
    fn sync() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_file = DataFile::new(temp_dir.path(), 0)?;
        assert_eq!(data_file.id, 0);

        let record = Record::normal("foo".into(), "baraaa".into());

        data_file.write_record(&record)?;

        data_file.sync()?;

        Ok(())
    }
}
