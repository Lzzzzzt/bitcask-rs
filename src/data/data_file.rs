use std::path::Path;

use crate::errors::BCResult;
use crate::file::io::{create_io_manager, IO};
use crate::DB_DATA_FILE_SUFFIX;

use super::log_record::{LogRecord, ReadLogRecord};

pub struct DataFile {
    pub(crate) id: u32,
    pub(crate) write_offset: u32,
    io: Box<dyn IO>,
}

impl DataFile {
    pub fn new(directory: impl AsRef<Path>, id: u32) -> BCResult<Self> {
        // construct complete filename
        let mut filename = directory.as_ref().to_path_buf();
        filename.push(format!("{:09}.{}", id, DB_DATA_FILE_SUFFIX));

        // init io manager
        let io = create_io_manager(filename)?;

        Ok(Self {
            id,
            write_offset: 0,
            io: Box::new(io),
        })
    }

    pub fn write_record(&mut self, record: &LogRecord) -> BCResult<u32> {
        let encoded = record.encode();
        let write_size = self.io.write(&encoded)?;
        self.write_offset += write_size;
        Ok(write_size)
    }

    /// read `ReadLogRecord` from data file by offset
    pub fn read_record(&self, offset: u32) -> BCResult<ReadLogRecord> {
        ReadLogRecord::decode(self.io.as_ref(), offset)
    }

    pub fn sync(&self) -> BCResult<()> {
        self.io.sync()
    }
}

#[cfg(test)]
mod tests {

    use crate::{data::log_record::LogRecord, errors::BCResult};

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

        let record = LogRecord::normal("foo".into(), "bar".into());

        let encoded_record = record.encode();
        let write_size = data_file.write_record(&record)?;
        assert_eq!(encoded_record.len() as u32, write_size);

        let record = LogRecord::normal("foo".into(), "".into());

        let encoded_record = record.encode();
        let write_size = data_file.write_record(&record)?;
        assert_eq!(encoded_record.len() as u32, write_size);

        let record = LogRecord::deleted("foo".into());

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
        let record = LogRecord::normal("foo".into(), "baraaa".into());

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(0)?;

        let mut size = read_record.size;

        assert_eq!(record.key, read_record.record.key);
        assert_eq!(record.value, read_record.record.value);
        assert_eq!(record.record_type, read_record.record.record_type);

        // value is empty
        let record = LogRecord::normal("foo".into(), Default::default());

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(size)?;
        size += read_record.size;

        assert_eq!(record.key, read_record.record.key);
        assert_eq!(record.value, read_record.record.value);
        assert_eq!(record.record_type, read_record.record.record_type);

        // type is deleted
        let record = LogRecord::deleted("foo".into());

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(size)?;

        assert_eq!(record.key, read_record.record.key);
        assert_eq!(record.value, read_record.record.value);
        assert_eq!(record.record_type, read_record.record.record_type);

        Ok(())
    }

    #[test]
    fn sync() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_file = DataFile::new(temp_dir.path(), 0)?;
        assert_eq!(data_file.id, 0);

        let record = LogRecord::normal("foo".into(), "baraaa".into());

        data_file.write_record(&record)?;

        data_file.sync()?;

        Ok(())
    }
}
