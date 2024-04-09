use std::collections::HashMap;
use std::sync::atomic::Ordering;

use parking_lot::RwLock;

use crate::config::WriteBatchConfig;
use crate::data::log_record::{Record, RecordDataType};
use crate::db::Engine;
use crate::errors::{BCResult, Errors};
use crate::utils::{check_key_valid, Key, Value};

/// ensure atomic write
pub struct WriteBatch<'a> {
    pub pending: RwLock<HashMap<Key, Record>>,
    pub engine: &'a Engine,
    pub config: WriteBatchConfig,
}

impl Engine {
    pub fn new_write_batch(&self, config: WriteBatchConfig) -> BCResult<WriteBatch> {
        Ok(WriteBatch {
            pending: Default::default(),
            engine: self,
            config,
        })
    }
}

impl<'a> WriteBatch<'a> {
    pub fn put(&self, key: Key, value: Value) -> BCResult<()> {
        check_key_valid(&key)?;

        // staging data in memory

        // here record key is empty, due to the hashmap has store the key
        // when commit, should restore the key to the record
        self.pending
            .write()
            .insert(key, Record::normal(Default::default(), value));

        Ok(())
    }

    pub fn del(&self, key: &[u8]) -> BCResult<()> {
        check_key_valid(key)?;

        let mut pending = self.pending.write();

        let real_index = self.engine.get_index(key);

        // if the data that store in this batch but not in the engine,
        // the delete the data in the batch
        if real_index.get(key).is_none() && pending.contains_key(key) {
            pending.remove(key);
            return Ok(());
        }

        // staging data in memory
        pending.insert(key.into(), Record::deleted(Default::default()));

        Ok(())
    }

    /// write the staging data into file, the update memory index
    pub fn commit(&mut self) -> BCResult<()> {
        let pending = std::mem::take(self.pending.get_mut());

        if pending.is_empty() {
            return Ok(());
        }
        if pending.len() as u32 > self.config.max_bacth_size {
            return Err(Errors::ExceedMaxBatchSize(pending.len()));
        }

        // get the global batch lock
        #[allow(unused)]
        let lock = self.engine.batch_lock.lock();

        // get the global unique batch id
        let seq = self.engine.batch_seq.fetch_add(1, Ordering::SeqCst);

        // store the record positon for update
        let mut record_and_position = Vec::with_capacity(pending.len());

        for (k, mut v) in pending {
            v.key = k;
            let record = v.enable_transaction(seq);

            let pos = self.engine.append_log_record(&record)?;

            record_and_position.push((record, pos));
        }

        // append batch finished record
        self.engine
            .append_log_record(&Record::batch_finished(seq))?;

        if self.config.sync_write {
            self.engine.sync()?;
        }

        // update memory index
        record_and_position
            .into_iter()
            .try_for_each(|(record, pos)| {
                let real_index = self.engine.get_index(&record.key);

                match record.record_type {
                    RecordDataType::Deleted => real_index.del(&record.key),
                    RecordDataType::Normal => real_index.put(record.key, pos),
                    RecordDataType::Commited => unreachable!(),
                }
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use fake::{faker::lorem::en::Sentence, Fake};

    use crate::config::Config;

    use super::*;

    #[test]
    fn batch_put() -> Result<(), Box<dyn Error>> {
        let temp_dir = tempfile::tempdir()?;
        let engine = Engine::open(Config::test_config(temp_dir.path().to_path_buf()))?;

        let mut batch = engine.new_write_batch(WriteBatchConfig::default())?;

        let gen_key = Sentence(16..64);
        let gen_value = Sentence(128..1024);

        // use batch but not commit
        let key1: Vec<u8> = gen_key.fake::<String>().into();
        let value1: Vec<u8> = gen_value.fake::<String>().into();
        let res = batch.put(key1.clone(), value1.clone());
        assert!(res.is_ok());
        let res = batch.put(
            gen_key.fake::<String>().into(),
            gen_value.fake::<String>().into(),
        );
        assert!(res.is_ok());

        let res = engine.get(&key1);
        assert!(matches!(res.unwrap_err(), Errors::KeyNotFound));

        batch.commit()?;
        let res = engine.get(&key1);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), value1);

        // check the commit seq num

        assert_eq!(batch.engine.batch_seq.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn batch_del() -> Result<(), Box<dyn Error>> {
        let temp_dir = tempfile::tempdir()?;
        let engine = Engine::open(Config::test_config(temp_dir.path().to_path_buf()))?;

        let mut batch = engine.new_write_batch(WriteBatchConfig::default())?;

        let gen_key = Sentence(16..64);
        let gen_value = Sentence(128..1024);

        let key1: Vec<u8> = gen_key.fake::<String>().into();
        let value1: Vec<u8> = gen_value.fake::<String>().into();
        let res = batch.put(key1.clone(), value1.clone());
        assert!(res.is_ok());

        let key2: Vec<u8> = gen_key.fake::<String>().into();
        let value2: Vec<u8> = gen_value.fake::<String>().into();
        let res = batch.put(key2.clone(), value2.clone());
        assert!(res.is_ok());

        assert!(batch.del(&key1).is_ok());

        batch.commit()?;

        let res = engine.get(&key1);
        assert!(matches!(res.unwrap_err(), Errors::KeyNotFound));

        let res = engine.get(&key2);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), value2);

        Ok(())
    }
}
