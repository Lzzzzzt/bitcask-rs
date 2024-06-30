use std::{collections::HashSet, sync::Arc};

use crate::data::log_record::{Record, RecordDataType};
use crate::db::Engine;
use crate::errors::{BCResult, Errors};
use crate::utils::check_key_valid;

use self::manager::TxnManager;

pub mod engine;
mod manager;

#[derive(Debug)]
pub(crate) struct Key {
    raw: Vec<u8>,
    version: u64,
}

impl Key {
    fn new(raw: Vec<u8>, version: u64) -> Self {
        Self { raw, version }
    }

    fn encode(mut self) -> Vec<u8> {
        self.raw.extend_from_slice(&self.version.to_be_bytes());
        self.raw
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TxnSearchType {
    Read,
    Write,
}

/// 事务
pub struct Transaction {
    engine: Arc<Engine>,
    manager: Arc<TxnManager>,
    version: u64,
    active_txn_id: HashSet<u64>,
}

impl Transaction {
    pub(crate) fn begin(engine: Arc<Engine>, manager: Arc<TxnManager>) -> Self {
        let version = manager.acquire_next_version();
        let active_txn_id = manager.add_txn(version);

        Self {
            engine,
            version,
            active_txn_id,
            manager,
        }
    }

    /// 写入数据
    /// ## 参数
    /// - key: 键
    /// - value: 值
    /// ## 返回
    /// - 成功返回 Ok(())
    /// - 失败返回 Err(Errors)
    pub fn put<T: Into<Vec<u8>>>(&self, key: T, value: T) -> BCResult<()> {
        let key = key.into();
        check_key_valid(&key)?;
        self.write(Record::normal(key, value.into()))
    }

    /// 删除数据
    /// ## 参数
    /// - key: 键
    /// ## 返回
    /// - 成功返回 Ok(())
    /// - 失败返回 Err(Errors)
    pub fn del<T: Into<Vec<u8>>>(&self, key: T) -> BCResult<()> {
        let key = key.into();
        check_key_valid(&key)?;

        match self.write(Record::deleted(key)) {
            Ok(_) => Ok(()),
            Err(Errors::KeyNotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// 获取数据
    /// ## 参数
    /// - key: 键
    /// ## 返回
    /// - 成功返回 Ok(Vec<u8>)
    /// - 失败返回 Err(Errors)
    pub fn get<T: AsRef<[u8]>>(&self, key: T) -> BCResult<Vec<u8>> {
        let key = key.as_ref();
        check_key_valid(key)?;

        let (position, _) = self.engine.txn_search(key, TxnSearchType::Read, self)?;

        let record = self.engine.get_record_with_position(position)?;

        match record.record_type {
            RecordDataType::Deleted => Err(Errors::KeyNotFound),
            RecordDataType::Normal => Ok(record.value().into()),
            RecordDataType::Commited => unreachable!(),
        }
    }

    /// 提交事务
    /// ## 返回
    /// - 成功返回 Ok(())
    /// - 失败返回 Err(Errors)
    pub fn commit(&self) -> BCResult<()> {
        // cleanup useless keys
        self.manager.remove_txn(self.version);
        self.manager.sync_to_file()?;
        self.engine.sync()
    }

    /// 回滚事务
    /// ## 返回
    /// - 成功返回 Ok(())
    /// - 失败返回 Err(Errors)
    pub fn rollback(&self) -> BCResult<()> {
        if let Some(keys) = self.manager.remove_txn(self.version) {
            for key in keys {
                self.engine.del(Key::new(key, self.version).encode())?;
            }
        }

        self.manager.sync_to_file()?;
        self.engine.sync()
    }

    pub(crate) fn is_visible(&self, version: u64) -> bool {
        if self.active_txn_id.contains(&version) {
            return false;
        }

        version <= self.version
    }

    fn write(&self, record: Record) -> BCResult<()> {
        let key = record.key;
        let value = record.value;

        match self.engine.txn_search(&key, TxnSearchType::Write, self) {
            Ok((_, version)) => {
                if version != self.version {
                    self.manager.mark_to_clean(version, key.clone())
                }
            }
            Err(Errors::TxnConflict) => return Err(Errors::TxnConflict),
            _ => (),
        }

        self.manager.update_txn(self.version, &key);

        let encoded_key = Key::new(key, self.version);

        let mut write_record = Record::normal(encoded_key.encode(), value);
        write_record.record_type = record.record_type;

        self.engine.txn_write(write_record)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{Config, IndexType},
        utils::tests::open,
    };

    use self::engine::TxnEngine;

    use super::*;

    fn transaction(index_type: IndexType) -> BCResult<()> {
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

        let engine = TxnEngine::new(Engine::open(config)?)?;

        // txn 0
        engine.update(|txn| {
            txn.put("a", "a1")?;
            txn.put("b", "b1")?;
            txn.put("c", "c1")?;
            txn.put("d", "d1")?;
            txn.put("e", "e1")?;
            Ok(())
        })?;

        // Time
        //  0  a1  b1  c1  d1  e1  committed
        //     a   b   c   d   e   Keys

        // txn 0 is commited, other txn can see the data
        engine.update(|txn| {
            let a1 = txn.get("a");
            assert!(a1.is_ok());
            assert_eq!(a1.unwrap(), b"a1");

            let b1 = txn.get("b");
            assert!(b1.is_ok());
            assert_eq!(b1.unwrap(), b"b1");

            let c1 = txn.get("c");
            assert!(c1.is_ok());
            assert_eq!(c1.unwrap(), b"c1");

            let d1 = txn.get("d");
            assert!(d1.is_ok());
            assert_eq!(d1.unwrap(), b"d1");

            let e1 = txn.get("e");
            assert!(e1.is_ok());
            assert_eq!(e1.unwrap(), b"e1");
            Ok(())
        })?;

        // Time
        //  1                      committed
        //  0  a1  b1  c1  d1  e1  committed
        //     a   b   c   d   e   Keys

        let txn2 = engine.begin_transaction();
        // txn2 change some data
        txn2.put("a", "a2")?;
        txn2.put("e", "e2")?;
        // txn2 can see the data changed by itself
        let a2 = txn2.get("a");
        assert!(a2.is_ok());
        assert_eq!(a2.unwrap(), b"a2");
        let e2 = txn2.get("e");
        assert!(e2.is_ok());
        assert_eq!(e2.unwrap(), b"e2");

        // Time
        //  2  a2              e2  uncommited
        //  1                      committed
        //  0  a1  b1  c1  d1  e1  committed
        //     a   b   c   d   e   Keys

        // txn3
        let txn3 = engine.begin_transaction();
        txn3.del("b")?;

        // txn3 can see the txn0's data rather than txn2's
        let a1 = txn3.get("a");
        assert!(a1.is_ok());
        assert_eq!(a1.unwrap(), b"a1");
        let e1 = txn3.get("e");
        assert!(e1.is_ok());
        assert_eq!(e1.unwrap(), b"e1");
        // txn3 can't change "a"
        let a3 = txn3.put("a", "a3");
        assert!(a3.is_err());
        assert!(matches!(a3.unwrap_err(), Errors::TxnConflict));
        // txn3 change "c"
        txn3.put("c", "c3")?;

        // Time
        //  3      x   c3          uncommited
        //  2  a2              e2  uncommited
        //  1                      committed
        //  0  a1  b1  c1  d1  e1  committed
        //     a   b   c   d   e   Keys

        // txn2 commmit
        txn2.commit()?;

        // Time
        //  3      x   c3          uncommited
        //  2  a2              e2  committed
        //  1                      committed
        //  0  a1  b1  c1  d1  e1  committed
        //     a   b   c   d   e   Keys

        // txn3 still can't see the txn2's data (RR)
        let a1 = txn3.get("a");
        assert!(a1.is_ok());
        assert_eq!(a1.unwrap(), b"a1");
        let e1 = txn3.get("e");
        assert!(e1.is_ok());
        assert_eq!(e1.unwrap(), b"e1");

        // txn3 can see the data changed by itself
        let c3 = txn3.get("c");
        assert!(c3.is_ok());
        assert_eq!(c3.unwrap(), b"c3");

        txn3.rollback()?;

        // Time
        //  3                      rollback
        //  2  a2              e2  committed
        //  1                      committed
        //  0  a1  b1  c1  d1  e1  committed
        //     a   b   c   d   e   Keys

        // txn3's change will not take effect
        engine.update(|txn| {
            let _ = txn.get("c");
            // assert!(c1.is_ok());
            // assert_eq!(c1.unwrap(), b"c1");
            Ok(())
        })?;

        // now there is no active transacion. will auto run the cleanup

        // Time
        //  3                      rollback
        //  2  a2              e2  committed
        //  1                      committed
        //  0      b1  c1  d1      committed
        //     a   b   c   d   e   Keys

        // the index of <a, a1>, <e, e1> should be deleted
        let e = engine.get_engine();
        assert!(e.get(Key::new(b"a".to_vec(), 0).encode()).is_err());
        assert!(e.get(Key::new(b"e".to_vec(), 0).encode()).is_err());
        assert!(e
            .get(Key::new(b"a".to_vec(), txn2.version).encode())
            .is_ok());
        assert!(e
            .get(Key::new(b"e".to_vec(), txn2.version).encode())
            .is_ok());

        engine.sync()?;

        Ok(())
    }

    fn cleanup_uncommit_transaction(index_type: IndexType) -> BCResult<()> {
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

        let engine = TxnEngine::new(Engine::open(config)?)?;

        let txn = engine.begin_transaction();

        txn.put("a", "a1")?;
        txn.put("b", "b1")?;
        txn.put("c", "c1")?;
        txn.put("d", "d1")?;
        txn.put("e", "e1")?;

        engine.close()?;

        let engine = TxnEngine::new(open(temp_dir.path().to_path_buf())?)?;

        assert!(engine.is_empty());
        assert!(engine.state().key_num == 0);

        Ok(())
    }

    #[test]
    fn transacion_use_btree() -> BCResult<()> {
        transaction(IndexType::BTree)?;
        Ok(())
    }

    #[test]
    fn transacion_use_hashmap() -> BCResult<()> {
        assert!(transaction(IndexType::HashMap).is_err());
        Ok(())
    }

    #[test]
    fn transacion_use_skiplist() -> BCResult<()> {
        transaction(IndexType::SkipList)?;
        Ok(())
    }

    #[test]
    fn cleanup_uncommit_transaction_btree() -> BCResult<()> {
        cleanup_uncommit_transaction(IndexType::BTree)?;
        Ok(())
    }

    #[test]
    fn cleanup_uncommit_transaction_skiplist() -> BCResult<()> {
        cleanup_uncommit_transaction(IndexType::SkipList)?;
        Ok(())
    }

    #[test]
    fn transacion_expire() -> BCResult<()> {
        Ok(())
    }
}
