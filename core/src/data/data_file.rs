use std::path::{Path, PathBuf};

use crate::errors::BCResult;
use crate::file::io::{create_io_manager, IO};

use crate::file::system_file::SystemFile;
// use crate::file::system_file::SystemFile;
use crate::{DB_DATA_FILE_SUFFIX, DB_HINT_FILE, DB_MERGE_FIN_FILE};

use super::log_record::{ReadRecord, Record};

pub struct DataFile {
    pub(crate) id: u32,
    pub(crate) write_offset: u32,
    io: SystemFile,
}

impl DataFile {
    pub fn new<P: AsRef<Path>>(directory: P, id: u32) -> BCResult<Self> {
        // construct complete filename
        let filename = data_file_name(directory, id);

        Ok(Self {
            id,
            write_offset: 0,
            io: create_io_manager(filename)?,
        })
    }

    pub fn hint_file<P: AsRef<Path>>(path: P) -> BCResult<Self> {
        let filename = path.as_ref().join(DB_HINT_FILE);

        Ok(Self {
            id: 0,
            write_offset: 0,
            io: create_io_manager(filename)?,
        })
    }

    pub fn merge_finish_file<P: AsRef<Path>>(path: P) -> BCResult<Self> {
        let filename = path.as_ref().join(DB_MERGE_FIN_FILE);

        Ok(Self {
            id: 0,
            write_offset: 0,
            io: create_io_manager(filename)?,
        })
    }

    pub fn write_record(&mut self, record: &Record) -> BCResult<u32> {
        let encoded = record.encode();
        let write_size = self.io.write(&encoded, self.write_offset)?;
        self.write_offset += write_size;
        Ok(write_size)
    }

    /// read `ReadLogRecord` from data file by offset
    pub fn read_record(&self, offset: u32) -> BCResult<ReadRecord> {
        ReadRecord::decode(&self.io, offset)
    }

    pub fn read_record_with_size(&self, offset: u32, size: u32) -> BCResult<ReadRecord> {
        let mut data = vec![0; size as usize];
        self.io.read(&mut data, offset)?;
        ReadRecord::decode_vec(data)
    }

    pub fn sync(&self) -> BCResult<()> {
        self.io.sync()
    }
}

pub(crate) fn data_file_name<P: AsRef<Path>>(p: P, id: u32) -> PathBuf {
    p.as_ref()
        .join(format!("{:09}.{}", id, DB_DATA_FILE_SUFFIX))
}

#[cfg(test)]
mod tests {

    use crate::{data::log_record::Record, errors::BCResult};

    use super::DataFile;

    #[test]
    fn new() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_file1 = DataFile::new(temp_dir.path(), 0)?;
        assert_eq!(data_file1.id, 0);

        let data_file2 = DataFile::new(temp_dir.path(), 0)?;
        assert_eq!(data_file2.id, 0);

        let data_file3 = DataFile::new(temp_dir.path(), 666)?;
        assert_eq!(data_file3.id, 666);

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
