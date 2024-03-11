use std::sync::atomic::AtomicU64;
use std::{collections::HashMap, fs, path::Path};

use parking_lot::{Mutex, RwLock};

use crate::config::Config;
use crate::data::data_file::DataFile;
use crate::data::log_record::{
    LogRecord, ReadLogRecord, RecordBatchState, RecordPosition, RecordType,
};
use crate::errors::{BCResult, Errors};
use crate::index::{create_indexer, Indexer};
use crate::utils::{check_key_valid, Key, Value};
use crate::{DB_DATA_FILE_SUFFIX, DB_MERGE_FIN_FILE};

pub struct DBEngine {
    /// Only used for update index
    fids: Vec<u32>,
    pub(crate) config: Config,
    pub(crate) active: RwLock<DataFile>,
    pub(crate) index: Box<dyn Indexer>,
    pub(crate) archive: RwLock<HashMap<u32, DataFile>>,
    pub(crate) batch_lock: Mutex<()>,
    pub(crate) batch_seq: AtomicU64,
    pub(crate) merge_lock: Mutex<()>,
}

impl DBEngine {
    /// Open the Bitcask DBEngine
    pub fn open(config: Config) -> BCResult<Self> {
        // check the config
        config.check()?;

        // check the data file directory is existed, if not, then create it
        fs::create_dir_all(&config.db_path).map_err(|e| {
            Errors::CreateDBDirFailed(config.db_path.to_string_lossy().to_string(), e)
        })?;

        Self::load_merged_file(&config.db_path)?;

        let mut data_files = load_data_file(&config.db_path)?;

        let active_file = match data_files.pop() {
            Some(file) => file,
            None => DataFile::new(&config.db_path, 0)?,
        };

        let data_fids: Vec<u32> = data_files.iter().map(|f| f.id).collect();

        let file_with_id = data_fids.iter().cloned().zip(data_files);

        let active_file_id = active_file.id;

        let mut engine = Self {
            index: Box::new(create_indexer(&config.index_type)),
            active: RwLock::new(active_file),
            archive: RwLock::new(HashMap::from_iter(file_with_id)),
            fids: data_fids,
            config,
            batch_lock: Mutex::new(()),
            batch_seq: AtomicU64::new(1),
            merge_lock: Mutex::new(()),
        };

        // put the active file id into file ids
        engine.fids.push(active_file_id);

        engine.load_index_from_data_file()?;

        Ok(engine)
    }

    /// Store the key-value set
    /// ## Parameter
    /// + `key`: Bytes, should not be empty
    /// + `value`: Bytes
    /// ## Return Value
    /// will return `Err` when `key` is empty
    pub fn put(&self, key: Key, value: Value) -> BCResult<()> {
        // make sure the key is valid
        check_key_valid(&key)?;

        // construct the `LogRecord`
        let record = LogRecord::normal(key, value);

        // append the record in the data file
        let record_positoin = self.append_log_record(&record)?;

        // update in-memory index
        self.index
            .put(record.key, record_positoin)
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;
        Ok(())
    }

    /// Query the key-value set
    /// ## Parameter
    /// + `key`: Bytes, should not be empty
    /// ## Return Value
    /// will return `Err` when `key` is empty
    pub fn get(&self, key: &[u8]) -> BCResult<Vec<u8>> {
        // make sure the key is valid
        check_key_valid(key)?;

        // fecth log record positon with key
        let record_position = self.index.get(key).ok_or(Errors::KeyNotFound)?;

        let target_file_id = record_position.fid;

        // get record
        let ReadLogRecord { record, .. } =
            self.get_record(target_file_id, record_position.offset)?;

        // if this record is deleted, return `KeyNotFound`
        if matches!(record.record_type, RecordType::Deleted) {
            Err(Errors::KeyNotFound)
        } else {
            Ok(record.value)
        }
    }

    /// Delete the key-value set
    /// ## Parameter
    /// + `key`: Bytes, should not be empty
    pub fn del(&self, key: &[u8]) -> BCResult<()> {
        check_key_valid(key)?;

        match self.index.get(key) {
            Some(_) => (),
            None => return Ok(()),
        };

        let record = LogRecord::deleted(key.to_vec());

        self.append_log_record(&record)?;

        self.index
            .del(key)
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;

        Ok(())
    }

    pub fn sync(&self) -> BCResult<()> {
        self.active.read().sync()
    }

    pub(crate) fn append_log_record(&self, record: &LogRecord) -> BCResult<RecordPosition> {
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

        if self.config.sync_write {
            active_file.sync()?;
        }

        // construct in-memory index infomation
        Ok(RecordPosition {
            fid: active_file.id,
            offset: write_offset,
        })
    }

    /// load index information from data file, will traverse all the data file, then process the record in order
    fn load_index_from_data_file(&mut self) -> BCResult<()> {
        if self.fids.is_empty() {
            return Ok(());
        }

        let mut merged = false;
        let mut unmerged_fid = 0;
        let merge_finish_file = self.config.db_path.join(DB_MERGE_FIN_FILE);

        if merge_finish_file.is_file() {
            let merge_finish_file = DataFile::merge_finish_file(merge_finish_file)?;
            let ReadLogRecord { record, .. } = merge_finish_file.read_record(0)?;
            let fid_bytes = record.value;
            unmerged_fid =
                u32::from_be_bytes([fid_bytes[0], fid_bytes[1], fid_bytes[2], fid_bytes[3]]);
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
        batched: &mut HashMap<u64, Vec<LogRecord>>,
    ) -> BCResult<u32> {
        let mut offset = 0;
        loop {
            let (record_len, record) = match self.get_record(id, offset) {
                Ok(ReadLogRecord { record, size }) => (size, record),
                Err(Errors::DataFileEndOfFile) => break,
                Err(e) => return Err(e),
            };

            let record_position = RecordPosition::new(id, offset);

            match record.batch_state {
                RecordBatchState::Enable(state) => match record.record_type {
                    RecordType::Commited => batched
                        .remove(&state.get())
                        .unwrap()
                        .into_iter()
                        .try_for_each(|record| self.update_index(record, record_position)),
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

        Ok(offset)
    }

    pub(crate) fn update_index(
        &self,
        record: LogRecord,
        record_position: RecordPosition,
    ) -> BCResult<()> {
        match record.record_type {
            RecordType::Deleted => self.index.del(&record.key),
            RecordType::Normal => self.index.put(record.key, record_position),
            RecordType::Commited => unreachable!(),
        }
    }

    fn get_record(&self, id: u32, offset: u32) -> BCResult<ReadLogRecord> {
        let active_file = self.active.read();
        let old_files = self.archive.read();

        if active_file.id == id {
            active_file.read_record(offset)
        } else {
            old_files.get(&id).unwrap().read_record(offset)
        }
    }
}

fn load_data_file(dir: impl AsRef<Path>) -> BCResult<Vec<DataFile>> {
    let directory = fs::read_dir(&dir)
        .map_err(|e| Errors::OpenDBDirFailed(dir.as_ref().to_string_lossy().to_string(), e))?;

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
            .map_err(|_| Errors::DataFileMayBeDamaged(filename))?;

        data_files.push(DataFile::new(&dir, id)?);
    }

    data_files.sort_unstable_by_key(|f| f.id);

    Ok(data_files)
}

#[cfg(test)]
mod tests {

    use fake::faker::lorem::en::{Sentence, Word};
    use fake::Fake;

    use crate::utils::tests::open;

    use super::*;

    #[test]
    fn put() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf());
        let word = Word();
        let sentence = Sentence(64..65);

        // put one normal record
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone().into(), value.clone().into());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        let res_value = res.unwrap();
        assert_eq!(value.as_bytes(), res_value);

        // put the same key with different value
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone().into(), value.clone().into());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        let res_value = res.unwrap();
        assert_eq!(value.as_bytes(), res_value);

        // key is empty
        let res = engine.put(Default::default(), value.clone().into());
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), Errors::KeyEmpty));

        // value is empty
        let key: String = word.fake();
        let value: String = Default::default();
        let res = engine.put(key.clone().into(), value.clone().into());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        let res_value = res.unwrap();
        assert_eq!(value.as_bytes(), res_value);

        // write until create new active file
        for _ in 0..200000 {
            let key: String = word.fake();
            let value: String = sentence.fake();
            engine.put(key.into(), value.into())?;
        }

        // reboot, then put
        // TODO: close the old db
        drop(engine);
        let engine = open(temp_dir.path().to_path_buf());

        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone().into(), value.clone().into());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        let res_value = res.unwrap();
        assert_eq!(value.as_bytes(), res_value);

        Ok(())
    }

    #[test]
    fn get() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf());
        let word = Word();
        let sentence = Sentence(64..65);

        // get one normal record
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone().into(), value.clone().into());
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
        let res = engine.put(key.clone().into(), value.clone().into());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(value.as_bytes(), res.unwrap());

        // read after delete
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone().into(), value.clone().into());
        assert!(res.is_ok());
        let res = engine.del(key.as_bytes());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), Errors::KeyNotFound));

        // read from old data files instead of active file
        let old_key: String = "111".into();
        let old_value: String = "222".into();
        engine.put(old_key.clone().into(), old_value.clone().into())?;

        for _ in 0..200000 {
            let key: String = word.fake();
            let value: String = sentence.fake();
            engine.put(key.into(), value.into())?;
        }
        let res = engine.get(old_key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(old_value, String::from_utf8(res.unwrap()).unwrap());

        // reopen the db, get the old record
        let engine = open(temp_dir.path().to_path_buf());
        let res = engine.get(old_key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(old_value, String::from_utf8(res.unwrap()).unwrap());

        Ok(())
    }

    #[test]
    fn del() {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf());
        let word = Word();
        let sentence = Sentence(64..65);

        // delete a exist key
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone().into(), value.clone().into());
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

        let res = engine.put(key.clone().into(), value.clone().into());
        assert!(res.is_ok());

        let res = engine.del(key.as_bytes());
        assert!(res.is_ok());

        let res = engine.put(key.clone().into(), value.clone().into());
        assert!(res.is_ok());

        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(value.as_bytes(), res.unwrap());
    }
}
