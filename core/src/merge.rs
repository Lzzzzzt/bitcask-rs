use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;

use crate::consts::*;
use crate::data::data_file::DataFile;
use crate::data::log_record::{Record, RecordPosition};
use crate::db::Engine;
use crate::errors::{BCResult, Errors};
use crate::utils::{data_file_name, merge_path};

impl Engine {
    pub fn merge(&self) -> BCResult<()> {
        if self.is_empty() {
            return Ok(());
        }

        #[allow(unused)]
        let lock = self.merge_lock.try_lock().ok_or(Errors::DBIsMerging)?;

        let merge_directory = merge_path(&self.config.db_path);

        if merge_directory.is_dir() {
            std::fs::remove_dir_all(&merge_directory).unwrap();
        }

        std::fs::create_dir_all(&merge_directory).map_err(|e| {
            Errors::CreateMergeDirFailed(
                merge_directory
                    .to_string_lossy()
                    .to_string()
                    .into_boxed_str(),
                e,
            )
        })?;

        let merge_files = self.get_merge_files(&merge_directory)?;
        let merge_engine = MergeEngine::new(&merge_directory, self.config.file_size_threshold)?;
        let mut hint = DataFile::hint_file(&merge_directory)?;

        for file in merge_files.iter() {
            let mut offset = 0;
            loop {
                let (size, mut record) = match file.read_record(offset) {
                    Ok(record) => (record.size(), record.into_log_record()),
                    Err(Errors::DataFileEndOfFile) => break,
                    Err(e) => return Err(e),
                };

                if let Some(pos) = self.get_index(&record.key).get(&record.key) {
                    if pos.fid == file.id && pos.offset == offset && !record.is_expire() {
                        record.disable_batch();
                        let position = merge_engine.append_log_record(&record)?;
                        hint.write_record(&Record::normal(record.key, position.encode()))?;
                    }
                }

                offset += size;
            }
        }

        hint.sync()?;
        merge_engine.sync()?;

        let mut merge_finish_file = DataFile::merge_finish_file(&merge_directory)?;
        let unmerged_fid = merge_files.last().unwrap().id + 1;
        let merge_finish_record = Record::merge_finished(unmerged_fid);
        merge_finish_file.write_record(&merge_finish_record)?;

        merge_finish_file.sync()?;

        Ok(())
    }

    fn get_merge_files<P: AsRef<Path>>(&self, path: P) -> BCResult<Vec<DataFile>> {
        let mut archive = self.archive.write();

        // create a new active file for write
        let mut active_file = self.active.write();
        // sync the old active file
        active_file.sync()?;

        let active_fid = active_file.id;
        let old_active_file =
            std::mem::replace(&mut *active_file, DataFile::new(&path, active_fid + 1)?);

        // store the old active file in the archive
        archive.insert(active_fid, old_active_file);
        // store the old active file id

        let merge_file = archive
            .keys()
            .map(|k| DataFile::new(&path, *k))
            .try_collect()?;

        Ok(merge_file)
    }

    pub(crate) fn load_merged_file<P: AsRef<Path>>(dir: P) -> BCResult<()> {
        let path = merge_path(&dir);
        if !path.is_dir() {
            return Ok(());
        }

        let directory = std::fs::read_dir(&path).map_err(|e| {
            Errors::OpenDBDirFailed(path.to_string_lossy().to_string().into_boxed_str(), e)
        })?;

        let merged_filenames: Vec<String> = directory
            .filter_map(|f| f.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|filename| filename.ends_with(DB_DATA_FILE_SUFFIX))
            .filter(|filename| {
                filename.starts_with(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'])
            })
            .collect();

        let mut merge_finished = false;

        merged_filenames.iter().for_each(|f| {
            (f == DB_MERGE_FIN_FILE).then(|| merge_finished = true);
        });

        if !merge_finished {
            fs::remove_dir_all(&path).unwrap();
            return Ok(());
        }

        let merge_finish_file = DataFile::merge_finish_file(&path)?;
        let record = merge_finish_file.read_record(0)?;
        let bytes = record.value().first_chunk().unwrap();
        let unmerged_id = u32::from_be_bytes(*bytes);

        // remove merged file
        for fid in 0..unmerged_id {
            let filename = data_file_name(&path, fid);
            if filename.is_file() {
                std::fs::remove_file(filename).unwrap();
            }
        }

        // cp merged file to db path
        for f in merged_filenames {
            let src = path.join(&f);
            let dst = dir.as_ref().join(&f);
            std::fs::rename(src, dst).unwrap();
        }

        std::fs::remove_dir(path).unwrap();

        Ok(())
    }
}

struct MergeEngine {
    pub(crate) merge_path: PathBuf,
    pub(crate) file_size_threshold: u32,
    pub(crate) active: RwLock<DataFile>,
    pub(crate) archive: RwLock<HashMap<u32, DataFile>>,
}

impl MergeEngine {
    pub(crate) fn new<P: AsRef<Path>>(merge_path: P, file_size_threshold: u32) -> BCResult<Self> {
        Ok(Self {
            active: RwLock::new(DataFile::new(&merge_path, 0)?),
            merge_path: merge_path.as_ref().into(),
            file_size_threshold,
            archive: Default::default(),
        })
    }

    pub(crate) fn append_log_record(&self, record: &Record) -> BCResult<RecordPosition> {
        let db_path = &self.merge_path;

        let encoded_record_len = record.calculate_encoded_length() as u32;

        // fetch active file
        let mut active_file = self.active.write();

        // check current active file size is small then config.data_file_size
        if active_file.write_offset + encoded_record_len > self.file_size_threshold {
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

        // construct in-memory index infomation
        Ok(RecordPosition {
            fid: active_file.id,
            offset: write_offset,
            size: write_size,
        })
    }

    pub fn sync(&self) -> BCResult<()> {
        self.active.read().sync()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use fake::{
        faker::lorem::en::{Sentence, Word},
        Fake,
    };

    use crate::utils::tests::open;

    use super::*;

    #[test]
    fn merge() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;
        let word = Word();
        let sentence = Sentence(64..65);

        assert!(engine.merge().is_ok());

        let test_keys = (0..1024).map(|_| word.fake::<String>());
        let test_vals = (0..1024).map(|_| sentence.fake::<String>());

        for (k, v) in test_keys.clone().zip(test_vals) {
            engine.put(k.into_bytes(), v.into_bytes())?;
        }

        for (i, k) in test_keys.enumerate() {
            if i != 32 {
                engine.del(k.as_bytes())?;
            }
        }

        let res = engine.merge();
        assert!(res.is_ok());

        engine.close()?;

        open(temp_dir.path().to_path_buf())?;

        drop(temp_dir);

        Ok(())
    }

    #[test]
    fn merge_multithread() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf())?;
        let word = Word();
        let sentence = Sentence(64..65);

        let test_keys = (0..1024).map(|_| word.fake::<String>());
        let test_vals = (0..1024).map(|_| sentence.fake::<String>());

        for (k, v) in test_keys.clone().zip(test_vals) {
            engine.put(k.into_bytes(), v.into_bytes())?;
        }

        for (i, k) in test_keys.enumerate() {
            if i != 32 {
                engine.del(k.as_bytes())?;
            }
        }

        let engine = Arc::new(engine);

        let e = Arc::clone(&engine);
        let merge_handler = thread::spawn(move || {
            let res = e.merge();
            assert!(res.is_ok());
        });

        let put_handler = thread::spawn(move || {
            let test_keys = (0..1024).map(|_| word.fake::<String>());
            let test_vals = (0..1024).map(|_| sentence.fake::<String>());

            for (k, v) in test_keys.clone().zip(test_vals) {
                engine.put(k.into_bytes(), v.into_bytes()).unwrap();
            }
        });

        merge_handler.join().unwrap();
        put_handler.join().unwrap();

        Ok(())
    }

    #[test]
    fn merge_dir_is_exist() {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf()).unwrap();

        let merge_path = merge_path(&engine.config.db_path);
        std::fs::create_dir_all(merge_path).unwrap();

        engine.put("foo", "bar").unwrap();

        let res = engine.merge();
        assert!(res.is_ok());
    }

    #[test]
    fn no_permission_for_create_merge_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf()).unwrap();

        let mut perms = std::fs::metadata(&engine.config.db_path)
            .unwrap()
            .permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&engine.config.db_path, perms).unwrap();

        engine.put("foo", "bar").unwrap();

        let res = engine.merge();
        assert!(matches!(
            res.unwrap_err(),
            Errors::CreateMergeDirFailed(_, _)
        ));
    }
}
