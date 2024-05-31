use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use std::{fs, path::Path};

use bytesize::ByteSize;
use flate2::write::GzEncoder;
use flate2::Compression;
use fs4::FileExt;
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};

use crate::config::Config;
use crate::consts::*;
use crate::data::data_file::DataFile;
use crate::data::log_record::*;
use crate::errors::*;
use crate::index::*;
use crate::transaction::{Transaction, TxnSearchType};
use crate::utils::*;

#[derive(Clone, Copy, Debug)]
pub struct EngineState {
    pub data_file_num: usize,
    pub key_num: usize,
    pub reclaimable_size: ByteSize,
    pub disk_size: ByteSize,
}

impl From<EngineState> for std::collections::HashMap<String, String> {
    fn from(s: EngineState) -> Self {
        std::collections::HashMap::from([
            ("Datafile num".to_string(), s.data_file_num.to_string()),
            ("Key num".to_string(), s.key_num.to_string()),
            (
                "Reclaimable Size".to_string(),
                s.reclaimable_size.to_string(),
            ),
            ("Disk Size".to_string(), s.disk_size.to_string()),
        ])
    }
}

pub struct Engine {
    /// Only used for update index
    fids: Vec<u32>,

    pub(crate) config: Config,

    pub(crate) active: RwLock<DataFile>,
    pub(crate) read_active: RwLock<DataFile>,
    pub(crate) archive: RwLock<HashMap<u32, DataFile>>,

    pub(crate) index: Vec<Box<dyn Indexer>>,

    pub(crate) batch_lock: Mutex<()>,
    pub(crate) batch_seq: AtomicU64,

    pub(crate) merge_lock: Mutex<()>,

    pub(crate) lock_file: File,
    pub(crate) bytes_written: AtomicUsize,
    pub(crate) reclaimable: AtomicUsize,
}

impl Engine {
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

        let mut data_fids = load_datafile_id(&config.db_path)?;

        let active_file = data_fids
            .pop()
            .map(|id| DataFile::new(&config.db_path, id))
            .unwrap_or(DataFile::new(&config.db_path, 0))?;

        let data_files: Vec<DataFile> = data_fids
            .iter()
            .map(|id| {
                if config.start_with_mmap {
                    DataFile::new_mapped(&config.db_path, *id)
                } else {
                    DataFile::new(&config.db_path, *id)
                }
            })
            .try_collect()?;

        let file_with_id = data_fids.iter().copied().zip(data_files).collect();

        let active_file_id = active_file.id;

        let read_active_file = DataFile::new(&config.db_path, active_file_id)?;

        let mut engine = Self {
            index: create_indexer(&config.index_type, config.index_num),
            active: RwLock::new(active_file),
            read_active: RwLock::new(read_active_file),
            archive: RwLock::new(file_with_id),
            fids: data_fids,
            batch_lock: Mutex::new(()),
            batch_seq: AtomicU64::new(1),
            merge_lock: Mutex::new(()),
            lock_file,
            config: config.clone(),
            bytes_written: AtomicUsize::new(0),
            reclaimable: AtomicUsize::new(0),
        };

        // put the active file id into file ids
        engine.fids.push(active_file_id);

        engine.load_index()?;

        if config.start_with_mmap {
            engine.reset_io_type()?;
        }

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
    pub fn put<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(&self, key: K, value: V) -> BCResult<()> {
        let key = key.into();
        let value = value.into();
        // make sure the key is valid
        check_key_valid(&key)?;

        // construct the `LogRecord`
        let record = Record::normal(key, value);

        // append the record in the data file
        let record_positoin = self.append_log_record(&record)?;

        // update in-memory index
        let pre_pos = self
            .get_index(&record.key)
            .put(record.key, record_positoin)
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;

        if let Some(pos) = pre_pos {
            self.update_reclaimable(pos.size);
        }
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
        let pre_pos = self
            .get_index(&record.key)
            .put(record.key, record_positoin)
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;

        if let Some(pos) = pre_pos {
            self.update_reclaimable(pos.size);
        }
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
        if record.is_expire() {
            self.get_index(key).del(key)?;
            return Err(Errors::KeyNotFound);
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
    pub fn del<T: Into<Value>>(&self, key: T) -> BCResult<()> {
        let key: Vec<_> = key.into();

        check_key_valid(&key)?;

        let index = self.get_index(&key);

        if !index.exist(&key) {
            return Ok(());
        }

        let record = Record::deleted(key);

        self.append_log_record(&record)?;

        let pre_pos = index
            .del(&record.key)
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;

        self.update_reclaimable(pre_pos.size);

        Ok(())
    }

    pub fn sync(&self) -> BCResult<()> {
        self.active.read().sync()
    }

    pub fn is_empty(&self) -> bool {
        self.index.iter().all(|i| i.is_empty())
    }

    pub fn state(&self) -> EngineState {
        let key_num = self.index.iter().map(|i| i.len()).sum();

        EngineState {
            data_file_num: self.archive.read().len() + 1,
            key_num,
            reclaimable_size: ByteSize::b(self.reclaimable.load(Ordering::SeqCst) as u64),
            disk_size: ByteSize::b(self.bytes_written.load(Ordering::SeqCst) as u64),
        }
    }

    pub fn backup<T: ToString>(&self, name: T, path: Option<PathBuf>) -> BCResult<()> {
        let target_path = if let Some(p) = path {
            p
        } else {
            self.config.db_path.clone()
        }
        .join(format!("{}.backup.tar.gz", name.to_string()));

        let target_file = File::create(target_path).map_err(Errors::CreateBackupFileFailed)?;

        let enc = GzEncoder::new(target_file, Compression::default());
        let mut tar = tar::Builder::new(enc);

        tar.append_dir(".", &self.config.db_path)
            .map_err(Errors::CreateBackupFileFailed)?;

        Ok(())
    }

    pub(crate) fn txn_write(&self, record: Record) -> BCResult<()> {
        // append the record in the data file
        let record_positoin = self.append_log_record(&record)?;

        // update in-memory index
        self.get_index(&record.key)
            .put(record.key, record_positoin)
            .map_err(|_| Errors::MemoryIndexUpdateFailed)?;

        Ok(())
    }

    pub(crate) fn txn_search<T: AsRef<[u8]>>(
        &self,
        prefix: T,
        search_type: TxnSearchType,
        txn: &Transaction,
    ) -> BCResult<(RecordPosition, u64)> {
        let prefix = prefix.as_ref();

        let index = self.get_index(prefix);

        index.transaction_prefix_search(prefix, search_type, txn)
    }

    pub(crate) fn append_log_record(&self, record: &Record) -> BCResult<RecordPosition> {
        let config = &self.config;

        let db_path = &config.db_path;

        let record_len = record.calculate_encoded_length() as u32;

        // fetch active file
        let mut active = self.active.write();

        // check current active file size is small then config.data_file_size
        if active.write_offset + record_len > config.file_size_threshold {
            // sync active file
            active.padding(config.file_size_threshold)?;
            active.sync()?;

            let pre_fid = active.id;
            // create new active file and store active file into old file
            let pre_active = std::mem::replace(&mut *active, DataFile::new(db_path, pre_fid + 1)?);

            // update the active file for read
            *self.read_active.write() = DataFile::new(db_path, pre_fid + 1)?;

            self.archive.write().insert(pre_fid, pre_active);
        }

        // append record into active file
        let write_offset = active.write_offset;
        let writted = active.write_record(record)?;

        self.bytes_written
            .fetch_add(writted as usize, Ordering::SeqCst);

        if config.sync_write
            && config.bytes_per_sync > 0
            && self.bytes_written.load(Ordering::SeqCst) / config.bytes_per_sync >= 1
        {
            active.sync()?;
        }

        // construct in-memory index infomation
        Ok(RecordPosition {
            fid: active.id,
            offset: write_offset,
            size: writted,
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
                            .expect("[Batch]: Should not be None")
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
        let index = self.get_index(record.key());

        let position = match record.record_type {
            RecordDataType::Deleted => Some(index.del(record.key())?),
            RecordDataType::Normal => index.put(record.key().into(), record_position)?,
            RecordDataType::Commited => unreachable!(),
        };

        if let Some(position) = position {
            self.update_reclaimable(position.size);
        }

        Ok(())
    }

    #[inline]
    pub(crate) fn update_reclaimable(&self, size: u32) {
        self.reclaimable.fetch_add(size as usize, Ordering::SeqCst);
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

    pub(crate) fn get_record_with_position(
        &self,
        position: RecordPosition,
    ) -> BCResult<ReadRecord> {
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

    pub(crate) fn reset_io_type(&self) -> BCResult<()> {
        let mut archive = self.archive.write();

        archive
            .iter_mut()
            .try_for_each(|(_, f)| f.reset_io_type(&self.config.db_path))?;

        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if self.close().ok().is_some() {}
    }
}

fn load_datafile_id(dir: impl AsRef<Path>) -> BCResult<Vec<u32>> {
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

        data_files.push(id);
    }

    data_files.sort_unstable();

    Ok(data_files)
}

#[cfg(test)]
mod tests {

    use std::thread::sleep;

    use fake::faker::lorem::en::{Sentence, Word};
    use fake::Fake;

    use crate::config::IndexType;
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
        let value: String = sentence.fake();
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
        let res = engine.put(String::default(), value.clone());
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

        assert!(File::open(format!("{}/000000000.bcdata", temp_dir.path().display())).is_ok());

        let engine = open(temp_dir.path().to_path_buf())?;

        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());
        let res = engine.get(key.as_bytes());
        assert!(res.is_ok());
        let res_value = res.unwrap();
        assert_eq!(value.as_bytes(), res_value);

        temp_dir.close().unwrap();

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

    fn state(index_type: IndexType) -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();

        let config = Config {
            file_size_threshold: 64 * 1000 * 1000,
            db_path: temp_dir.path().to_path_buf(),
            sync_write: false,
            bytes_per_sync: 0,
            index_type,
            index_num: 4,
            start_with_mmap: false,
        };

        let engine = Engine::open(config)?;
        let word = Word();
        let sentence = Sentence(64..65);

        let state = engine.state();

        assert_eq!(state.data_file_num, 1);
        assert_eq!(state.disk_size.as_u64(), 0);
        assert_eq!(state.key_num, 0);
        assert_eq!(state.reclaimable_size.as_u64(), 0);

        assert!(engine.is_empty());

        // put one normal record
        let key: String = word.fake();
        let value = sentence.fake::<String>();
        let res = engine.put(key.clone(), value.clone());
        assert!(res.is_ok());

        let state = engine.state();

        assert_eq!(state.data_file_num, 1);
        assert!(state.disk_size.as_u64() > 0);
        assert_eq!(state.key_num, 1);
        assert_eq!(state.reclaimable_size.as_u64(), 0);

        // del
        let res = engine.del(key.as_bytes());
        assert!(res.is_ok());
        let state = engine.state();

        assert_eq!(state.data_file_num, 1);
        assert!(state.disk_size.as_u64() > 0);
        assert_eq!(state.key_num, 0);
        assert!(state.reclaimable_size.as_u64() > 0);

        Ok(())
    }

    #[test]
    fn state_with_btree() -> BCResult<()> {
        state(IndexType::BTree)?;
        Ok(())
    }
    #[test]
    fn state_with_hashmap() -> BCResult<()> {
        state(IndexType::HashMap)?;
        Ok(())
    }
    #[test]
    fn state_with_skiplist() -> BCResult<()> {
        state(IndexType::SkipList)?;
        Ok(())
    }

    #[test]
    fn start_with_mmap() -> BCResult<()> {
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

        let config = Config {
            file_size_threshold: 64 * 1000 * 1000,
            db_path: temp_dir.path().to_path_buf(),
            sync_write: true,
            bytes_per_sync: 0,
            index_type: crate::config::IndexType::BTree,
            index_num: 4,
            start_with_mmap: true,
        };

        let engine = Engine::open(config)?;

        let res = engine.get(old_key.as_bytes());
        assert!(res.is_ok());
        assert_eq!(old_value, String::from_utf8(res.unwrap()).unwrap());

        let key: String = word.fake();
        let value: String = sentence.fake();
        engine.put(key, value)?;

        Ok(())
    }

    #[test]
    fn backup() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;

        let word = Word();
        let sentence = Sentence(64..65);

        for _ in 0..200000 {
            let key: String = word.fake();
            let value: String = sentence.fake();
            engine.put(key, value)?;
        }

        // don't provide path
        engine.backup("test", None)?;

        let path = temp_dir.path().join("test.backup.tar.gz");

        assert!(path.is_file());

        // provide path
        engine.backup("test", Some("/tmp".into()))?;

        let path: PathBuf = "/tmp/test.backup.tar.gz".into();

        assert!(path.is_file());

        Ok(())
    }
}
