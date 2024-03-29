use std::collections::HashMap;

use std::path::{Path, PathBuf};

use tokio::sync::RwLock;

use crate::data::data_file::{data_file_name, DataFile};
use crate::data::log_record::{Record, RecordPosition};
use crate::db::DBEngine;
use crate::errors::{BCResult, Errors};
use crate::utils::merge_path;
use crate::DB_DATA_FILE_SUFFIX;
use crate::DB_MERGE_FIN_FILE;

impl DBEngine {
    pub async fn merge(&self) -> BCResult<()> {
        // TODO: Check Engine is not empty

        #[allow(unused)]
        let lock = match self.merge_lock.try_lock() {
            Ok(lock) => lock,
            Err(_) => return Err(Errors::DBIsInUsing),
        };

        let merge_directory = merge_path(&self.config.db_path);

        if merge_directory.is_dir() {
            tokio::fs::remove_dir_all(&merge_directory).await.unwrap();
        }

        tokio::fs::create_dir_all(&merge_directory)
            .await
            .map_err(|e| {
                Errors::CreateMergeDirFailed(
                    merge_directory
                        .to_string_lossy()
                        .to_string()
                        .into_boxed_str(),
                    e,
                )
            })?;

        let merge_files = self.get_merge_files(&merge_directory).await?;
        let merge_engine = MergeEngine::new(&merge_directory, self.config.file_size_threshold)?;
        let mut hint = DataFile::hint_file(&merge_directory)?;

        for file in merge_files.iter() {
            let mut offset = 0;
            loop {
                let (size, mut record) = match file.read_record(offset).await {
                    Ok(record) => (record.size(), record.into_log_record()),
                    Err(Errors::DataFileEndOfFile) => break,
                    Err(e) => return Err(e),
                };

                if let Some(pos) = self.get_index(&record.key).get(&record.key) {
                    if pos.fid == file.id && pos.offset == offset {
                        record.disable_transaction();
                        let position = merge_engine.append_log_record(&record).await?;

                        hint.write_record(&Record::normal(record.key, position.encode()))
                            .await?;
                    }
                }

                offset += size;
            }
        }

        hint.sync().await?;
        merge_engine.sync().await?;

        let mut merge_finish_file = DataFile::merge_finish_file(&merge_directory)?;
        let unmerged_fid = merge_files.last().unwrap().id + 1;
        let merge_finish_record = Record::merge_finished(unmerged_fid);
        merge_finish_file.write_record(&merge_finish_record).await?;

        merge_finish_file.sync().await?;

        Ok(())
    }

    async fn get_merge_files<P: AsRef<Path>>(&self, path: P) -> BCResult<Vec<DataFile>> {
        let mut archive = self.archive.write().await;

        // create a new active file for write
        let mut active_file = self.active.write().await;
        // sync the old active file
        active_file.sync().await?;

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

    pub(crate) async fn load_merged_file<P: AsRef<Path>>(dir: P) -> BCResult<()> {
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
            .collect();

        let mut merge_finished = false;

        merged_filenames.iter().for_each(|f| {
            (f == DB_MERGE_FIN_FILE).then(|| merge_finished = true);
        });

        if !merge_finished {
            tokio::fs::remove_dir_all(&path).await.unwrap();
            return Ok(());
        }

        let merge_finish_file = DataFile::merge_finish_file(&path)?;
        let record = merge_finish_file.read_record(0).await?;
        let bytes = record.value().first_chunk().unwrap();
        let unmerged_id = u32::from_be_bytes(*bytes);

        // remove merged file
        for fid in 0..unmerged_id {
            let filename = data_file_name(&path, fid);
            if filename.is_file() {
                tokio::fs::remove_file(filename).await.unwrap();
            }
        }
        // cp merged file to db path
        for f in merged_filenames {
            let src = path.join(&f);
            let dst = dir.as_ref().join(&f);
            tokio::fs::rename(src, dst).await.unwrap();
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

    pub(crate) async fn append_log_record(&self, record: &Record) -> BCResult<RecordPosition> {
        let db_path = &self.merge_path;

        let encoded_record_len = record.calculate_encoded_length() as u32;

        // fetch active file
        let mut active_file = self.active.write().await;

        // check current active file size is small then config.data_file_size
        if active_file.write_offset + encoded_record_len > self.file_size_threshold {
            // sync active file
            active_file.sync().await?;

            let current_file_id = active_file.id;
            // create new active file and store active file into old file
            let current_active_file = std::mem::replace(
                &mut *active_file,
                DataFile::new(db_path, current_file_id + 1)?,
            );
            self.archive
                .write()
                .await
                .insert(current_file_id, current_active_file);
        }

        // append record into active file
        let write_offset = active_file.write_offset;
        active_file.write_record(record).await?;

        let write_size = active_file.write_offset - write_offset;

        // construct in-memory index infomation
        Ok(RecordPosition {
            fid: active_file.id,
            offset: write_offset,
            size: write_size,
        })
    }

    pub async fn sync(&self) -> BCResult<()> {
        self.active.read().await.sync().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fake::{
        faker::lorem::en::{Sentence, Word},
        Fake,
    };

    use crate::utils::tests::open;

    use super::*;

    #[tokio::test]
    async fn merge() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf()).await?;
        let word = Word();
        let sentence = Sentence(64..65);

        let test_keys = (0..1024).map(|_| word.fake::<String>());
        let test_vals = (0..1024).map(|_| sentence.fake::<String>());

        for (k, v) in test_keys.clone().zip(test_vals) {
            engine.put(k, v).await?;
        }

        for (i, k) in test_keys.enumerate() {
            if i != 32 {
                engine.del(k).await?;
            }
        }

        let res = engine.merge().await;
        assert!(res.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn merge_multithread() -> BCResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = open(temp_dir.path().to_path_buf()).await?;
        let word = Word();
        let sentence = Sentence(64..65);

        let test_keys = (0..1024).map(|_| word.fake::<String>());
        let test_vals = (0..1024).map(|_| sentence.fake::<String>());

        for (k, v) in test_keys.clone().zip(test_vals) {
            engine.put(k, v).await?;
        }

        for (i, k) in test_keys.enumerate() {
            if i != 32 {
                engine.del(k).await?;
            }
        }

        let engine = Arc::new(engine);

        let e = Arc::clone(&engine);
        let merge_handler = tokio::spawn(async move {
            let res = e.merge().await;
            assert!(res.is_ok());
        });

        let put_handler = tokio::spawn(async move {
            let test_keys = (0..1024).map(|_| word.fake::<String>());
            let test_vals = (0..1024).map(|_| sentence.fake::<String>());

            for (k, v) in test_keys.clone().zip(test_vals) {
                engine.put(k, v).await.unwrap();
            }
        });

        merge_handler.await.unwrap();
        put_handler.await.unwrap();

        Ok(())
    }
}
