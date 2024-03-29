use std::fs::File;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, path::Path};

use fs4::FileExt;
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};

use crate::config::Config;
use crate::data::data_file::DataFile;
use crate::data::log_record::{
    ReadRecord, Record, RecordBatchState, RecordDataType, RecordExpireState, RecordPosition,
};
use crate::errors::{BCResult, Errors};
use crate::index::{create_indexer, Indexer};
use crate::utils::{check_key_valid, key_hash, merge_path};
use crate::{DB_DATA_FILE_SUFFIX, DB_FILE_LOCK, DB_MERGE_FIN_FILE};

pub struct DBEngine {
    /// Only used for update index
    fids: Vec<u32>,
    pub(crate) config: Config,
    pub(crate) active: RwLock<DataFile>,
    pub(crate) index: Vec<Box<dyn Indexer>>,
    pub(crate) archive: RwLock<HashMap<u32, DataFile>>,
    pub(crate) batch_lock: Mutex<()>,
    pub(crate) batch_seq: AtomicU64,
    pub(crate) merge_lock: Mutex<()>,
    pub(crate) lock_file: File,
    pub(crate) bytes_written: AtomicUsize,
}

impl DBEngine {
    /// Open the Bitcask DBEngine
    pub fn open(config: Config) -> BCResult<Self> {
        // check the config
        config.check()?;

        // check the data file directory is existed, if not, then create it
        fs::create_dir_all(&config.db_path).map_err(|e| {
            Errors::CreateDBDirFailed(
                config
                    .db_path
                    .to_string_lossy()
                    .to_string()
                    .into_boxed_str(),
                e,
            )
        })?;

        // file lock, make sure the db dir is used by one engine
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(config.db_path.join(DB_FILE_LOCK))
            .map_err(|_| Errors::OpenLockFileFailed)?;

        lock_file
            .try_lock_exclusive()
            .map_err(|_| Errors::DBIsInUsing)?;

        Self::load_merged_file(&config.db_path)?;

        let mut data_files = load_data_file(&config.db_path)?;

        let active_file = data_files
            .pop()
            .unwrap_or(DataFile::new(&config.db_path, 0)?);

        let data_fids: Vec<u32> = data_files.iter().map(|f| f.id).collect();

        let file_with_id = data_fids.iter().copied().zip(data_files).collect();

        let active_file_id = active_file.id;

        let mut engine = Self {
            index: create_indexer(&config.index_type, config.index_num),
            active: RwLock::new(active_file),
            archive: RwLock::new(file_with_id),
            fids: data_fids,
            config,
            batch_lock: Mutex::new(()),
            batch_seq: AtomicU64::new(1),
            merge_lock: Mutex::new(()),
            lock_file,
            bytes_written: AtomicUsize::new(0),
        };

        // put the active file id into file ids
        engine.fids.push(active_file_id);

        engine.load_index()?;

        Ok(engine)
    }

    pub fn close(&self) -> BCResult<()> {
        if !self.config.db_path.is_dir() {
            return Ok(());
        }

        self.sync()?;
        self.lock_file.unlock().unwrap();
        Ok(())
    }

    /// Store the key-value set
    /// ## Parameter
    /// + `key`: Bytes, should not be empty
    /// + `value`: Bytes
    /// ## Return Value
    /// will return `Err` when `key` is empty
    pub fn put<T: Into<Vec<u8>>>(&self, key: T, value: T) -> BCResult<()> {
        let key = key.into();
        let value = value.into();
        // make sure the key is valid
        check_key_valid(&key)?;

        // construct the `LogRecord`
        let record = Record::normal(key, value);

        // append the record in the data file
        let record_positoin = self.append_log_record(&record)?;

        // update in-memory index
        self.get_index(&record.key)
            .put(record.key, record_positoin)
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;
        Ok(())
    }

    pub fn put_expire<T: Into<Vec<u8>>>(&self, key: T, value: T, expire: Duration) -> BCResult<()> {
        let key = key.into();
        let value = value.into();
        // make sure the key is valid
        check_key_valid(&key)?;

        // construct the `LogRecord`
        let record = Record::normal(key, value).expire(expire)?;

        // append the record in the data file
        let record_positoin = self.append_log_record(&record)?;

        // update in-memory index
        self.get_index(&record.key)
            .put(record.key, record_positoin)
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;
        Ok(())
    }

    /// Query the key-value set
    /// ## Parameter
    /// + `key`: Bytes, should not be empty
    /// ## Return Value
    /// will return `Err` when `key` is empty
    pub fn get<T: AsRef<[u8]>>(&self, key: T) -> BCResult<Vec<u8>> {
        let key = key.as_ref();
        // make sure the key is valid
        check_key_valid(key)?;

        // fecth log record positon with key
        let record_position = self.get_index(key).get(key).ok_or(Errors::KeyNotFound)?;

        // get record
        let record = self.get_record_with_position(record_position)?;

        // check the record is expired or not
        if let RecordExpireState::Enable(ts) = record.expire {
            let now = SystemTime::now();
            let expire = UNIX_EPOCH + Duration::from_micros(ts.get() as u64);
            if expire <= now {
                self.get_index(key).del(key)?;
                return Err(Errors::KeyNotFound);
            }
        }

        // if this record is deleted, return `KeyNotFound`
        if matches!(record.record_type, RecordDataType::Deleted) {
            Err(Errors::KeyNotFound)
        } else {
            Ok(record.value().into())
        }
    }

    /// Delete the key-value set
    /// ## Parameter
    /// + `key`: Bytes, should not be empty
    pub fn del<T: AsRef<[u8]>>(&self, key: T) -> BCResult<()> {
        let key = key.as_ref();

        check_key_valid(key)?;

        let real_index = self.get_index(key);
        match real_index.get(key) {
            Some(_) => (),
            None => return Ok(()),
        };

        let record = Record::deleted(key.to_vec());

        self.append_log_record(&record)?;

        real_index
            .del(key)
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;

        Ok(())
    }

    pub fn sync(&self) -> BCResult<()> {
        self.active.read().sync()
    }

    pub(crate) fn append_log_record(&self, record: &Record) -> BCResult<RecordPosition> {
        let db_path = &self.config.db_path;

        let encoded_record_len = record.calculate_encoded_length() as u32;

        // fetch active file
        let mut active_file = self.active.write();

        // check current active file size is small then config.data_file_size
        if active_file.write_offset + encoded_record_len > self.config.file_size_threshold {
            // sync active file
            active_file.sync()?;

            let current_file_id = active_file.id;
            // create new active file and store active file into old file
            let current_active_file = std::mem::replace(
                &mut *active_file,
                DataFile::new(db_path, current_file_id + 1)?,
            );

            self.archive
                .write()
                .insert(current_file_id, current_active_file);
        }

        // append record into active file
        let write_offset = active_file.write_offset;
        active_file.write_record(record)?;

        let write_size = active_file.write_offset - write_offset;

        self.bytes_written
            .fetch_add(write_size as usize, Ordering::SeqCst);

        if self.config.sync_write
            && self.config.bytes_per_sync > 0
            && self.bytes_written.load(Ordering::SeqCst) / self.config.bytes_per_sync >= 1
        {
            active_file.sync()?;
        }

        // construct in-memory index infomation
        Ok(RecordPosition {
            fid: active_file.id,
            offset: write_offset,
            size: write_size,
        })
    }

    fn load_index(&self) -> BCResult<()> {
        self.load_index_from_hint_file()?;
        self.load_index_from_data_file()
    }

    fn load_index_from_hint_file(&self) -> BCResult<()> {
        let path = merge_path(&self.config.db_path);

        if !path.is_dir() {
            return Ok(());
        }

        let hint_file = DataFile::hint_file(path)?;
        let mut offset = 0;

        loop {
            match hint_file.read_record(offset) {
                Ok(record) => {
                    offset += record.size();

                    self.get_index(record.key())
                        .put(record.key().into(), RecordPosition::decode(record.value()))?;
                }
                Err(Errors::DataFileEndOfFile) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    /// load index information from data file, will traverse all the data file, then process the record in order
    fn load_index_from_data_file(&self) -> BCResult<()> {
        if self.fids.is_empty() {
            return Ok(());
        }

        let mut merged = false;
        let mut unmerged_fid = 0;
        let merge_finish_file = self.config.db_path.join(DB_MERGE_FIN_FILE);

        if merge_finish_file.is_file() {
            let merge_finish_file = DataFile::merge_finish_file(merge_finish_file)?;
            let record = merge_finish_file.read_record(0)?;
            let fid_bytes = record.value().first_chunk::<4>().unwrap();
            unmerged_fid = u32::from_be_bytes(*fid_bytes);
            merged = true;
        }

        // self.file_ids will at least have the active file id, so unwarp directly.
        let (active_file_id, old_file_ids) = self.fids.split_last().unwrap();

        let mut batched_record = HashMap::new();

        // update index from old file
        for &id in old_file_ids {
            if merged && id < unmerged_fid {
                continue;
            }

            self.update_index_batched(id, &mut batched_record)?;
        }

        // update index from active file
        self.active.write().write_offset =
            self.update_index_batched(*active_file_id, &mut batched_record)?;

        Ok(())
    }

    fn update_index_batched(
        &self,
        id: u32,
        batched: &mut HashMap<u64, Vec<ReadRecord>>,
    ) -> BCResult<u32> {
        let mut offset = 0;
        let mut current_seq_no = self.batch_seq.load(Ordering::SeqCst);

        loop {
            let (record_len, record) = match self.get_record(id, offset) {
                Ok(record) => (record.size(), record),
                Err(Errors::DataFileEndOfFile) => break,
                Err(e) => return Err(e),
            };

            let record_position = RecordPosition::new(id, offset, record_len);

            match record.batch_state {
                RecordBatchState::Enable(state) => match record.record_type {
                    RecordDataType::Commited => {
                        batched
                            .remove(&state.get())
                            .unwrap()
                            .into_iter()
                            .try_for_each(|record| self.update_index(record, record_position))?;
                        if current_seq_no < state.get() {
                            current_seq_no = state.get();
                        }
                        Ok(())
                    }
                    _ => {
                        batched.entry(state.into()).or_default().push(record);
                        Ok(())
                    }
                },
                RecordBatchState::Disable => self.update_index(record, record_position),
            }
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;

            offset += record_len;
        }

        self.batch_seq.store(current_seq_no, Ordering::SeqCst);

        Ok(offset)
    }

    pub(crate) fn update_index(
        &self,
        record: ReadRecord,
        record_position: RecordPosition,
    ) -> BCResult<()> {
        let real_index = self.get_index(record.key());

        match record.record_type {
            RecordDataType::Deleted => real_index.del(record.key()),
            RecordDataType::Normal => real_index.put(record.key().into(), record_position),
            RecordDataType::Commited => unreachable!(),
        }
    }

    fn get_record(&self, id: u32, offset: u32) -> BCResult<ReadRecord> {
        let active_file = self.active.read();
        let old_files = self.archive.read();

        if active_file.id == id {
            active_file.read_record(offset)
        } else {
            old_files.get(&id).unwrap().read_record(offset)
        }
    }

    fn get_record_with_position(&self, position: RecordPosition) -> BCResult<ReadRecord> {
        let active_file = self.active.read();
        let old_files = self.archive.read();

        if active_file.id == position.fid {
            active_file.read_record_with_size(position.offset, position.size)
        } else {
            old_files
                .get(&position.fid)
                .unwrap()
                .read_record_with_size(position.offset, position.size)
        }
    }

    pub(crate) fn get_index(&self, key: &[u8]) -> &dyn Indexer {
        unsafe {
            self.index
                .get_unchecked(key_hash(key, self.config.index_num))
                .as_ref()
        }
    }
}

impl Drop for DBEngine {
    fn drop(&mut self) {
        if self.close().ok().is_some() {}
    }
}

fn load_data_file(dir: impl AsRef<Path>) -> BCResult<Vec<DataFile>> {
    let directory = fs::read_dir(&dir).map_err(|e| {
        Errors::OpenDBDirFailed(
            dir.as_ref().to_string_lossy().to_string().into_boxed_str(),
            e,
        )
    })?;

    let data_filenames: Vec<String> = directory
        .filter_map(|f| f.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|filename| filename.ends_with(DB_DATA_FILE_SUFFIX))
        .collect();

    let mut data_files = Vec::with_capacity(data_filenames.len());

    for filename in data_filenames {
        let id: u32 = filename
            .split_once('.')
            .unwrap()
            .0
            .parse()
            .map_err(|_| Errors::DataFileMayBeDamaged(filename.into_boxed_str()))?;

        data_files.push(DataFile::new(&dir, id)?);
    }

    data_files.sort_unstable_by_key(|f| f.id);

    Ok(data_files)
}

#[cfg(test)]
mod tests {

    use std::thread::sleep;

    use fake::faker::lorem::en::{Sentence, Word};
    use fake::Fake;

    use crate::utils::tests::open;

    use super::*;

    #[test]
    fn put() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;
        let word = Word();
        let sentence = Sentence(64..65);

        // put one normal record
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        let res_value = res.unwrap();
        assert_eq!(value.as_bytes(), res_value);

        // put the same key with different value
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        let res_value = res.unwrap();
        assert_eq!(value.as_bytes(), res_value);

        // key is empty
        let res = engine.put(Default::default(), value.clone());
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), Errors::KeyEmpty));

        // value is empty
        let key: String = word.fake();
        let value: String = Default::default();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        let res_value = res.unwrap();
        assert_eq!(value.as_bytes(), res_value);

        // write until create new active file
        for _ in 0..200000 {
            let key: String = word.fake();
            let value: String = sentence.fake();
            engine.put(key, value)?;
        }

        // reboot, then put
        engine.close()?;

        let engine = open(temp_dir.path().to_path_buf())?;

        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        let res = dbg!(res);
        assert!(res.is_ok());
        let res_value = res.unwrap();
        assert_eq!(value.as_bytes(), res_value);

        Ok(())
    }

    #[test]
    fn expire() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;
        let word = Word();
        let sentence = Sentence(64..65);

        // put one normal record
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put_expire(key.clone(), value.clone(), Duration::from_secs(3));
        assert!(res.is_ok());

        sleep(Duration::from_secs(1));

        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());

        sleep(Duration::from_secs(2));

        let res = engine.get(key.as_bytes());
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn get() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;
        let word = Word();
        let sentence = Sentence(64..65);

        // get one normal record
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(value.as_bytes(), res.unwrap());

        // get the inexistent key;
        let res = engine.get(word.fake::<String>().as_bytes());
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), Errors::KeyNotFound));

        // read after value is repeated put
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(value.as_bytes(), res.unwrap());

        // read after delete
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.del(key.as_bytes());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), Errors::KeyNotFound));

        // read from old data files instead of active file
        let old_key: String = "111".into();
        let old_value: String = "222".into();
        engine.put(old_key.clone(), old_value.clone())?;

        for _ in 0..200000 {
            let key: String = word.fake();
            let value: String = sentence.fake();
            engine.put(key, value)?;
        }
        let res = engine.get(old_key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(old_value, String::from_utf8(res.unwrap()).unwrap());

        engine.close()?;

        // reopen the db, get the old record
        let engine = open(temp_dir.path().to_path_buf())?;
        let res = engine.get(old_key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(old_value, String::from_utf8(res.unwrap()).unwrap());

        Ok(())
    }

    #[test]
    fn del() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;
        let word = Word();
        let sentence = Sentence(64..65);

        // delete a exist key
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());

        let res = engine.del(key.as_bytes());
        assert!(res.is_ok());

        let res = engine.get(key.as_bytes());
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), Errors::KeyNotFound));

        // delete a inexistent key
        let res = engine.del("111".as_bytes());
        assert!(res.is_ok());

        // delete a empty key
        let res = engine.del("".as_bytes());
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), Errors::KeyEmpty));

        // delete then put
        let key: String = word.fake();
        let value = sentence.fake::<String>();

        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());

        let res = engine.del(key.as_bytes());
        assert!(res.is_ok());

        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());

        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(value.as_bytes(), res.unwrap());

        Ok(())
    }

    #[test]
    fn close() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;
        let word = Word();
        let sentence = Sentence(64..65);

        // get one normal record
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(value.as_bytes(), res.unwrap());

        assert!(engine.close().is_ok());

        Ok(())
    }

    #[test]
    fn sync() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;
        let word = Word();
        let sentence = Sentence(64..65);

        // get one normal record
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(value.as_bytes(), res.unwrap());

        assert!(engine.sync().is_ok());
        assert!(engine.close().is_ok());

        Ok(())
    }

    #[test]
    fn file_lock() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;
        let word = Word();
        let sentence = Sentence(64..65);

        // get one normal record
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(value.as_bytes(), res.unwrap());

        let engine2 = open(temp_dir.path().to_path_buf());
        assert!(engine2.is_err());

        engine.close()?;

        let engine2 = open(temp_dir.path().to_path_buf());
        assert!(engine2.is_ok());

        Ok(())
    }
}
