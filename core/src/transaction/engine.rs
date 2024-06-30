use std::sync::Arc;
use std::thread::{self};

use crossbeam_channel::unbounded;

use crate::config::IndexType;
use crate::errors::Errors;
use crate::{
    db::{Engine, EngineState},
    errors::BCResult,
};

use super::{manager::TxnManager, Key, Transaction};

pub struct TxnEngine {
    engine: Arc<Engine>,
    manager: Arc<TxnManager>,
}

impl TxnEngine {
    /// 创建一个新的事务引擎
    /// ## 参数
    /// - `engine`: 数据库引擎
    pub fn new(engine: Engine) -> BCResult<Self> {
        if engine.config.index_type == IndexType::HashMap {
            return Err(Errors::TxnHashmapError);
        }

        let (tx, rx) = unbounded();
        let manager = TxnManager::from_file_or_init(engine.config.clone(), tx)?;

        // delete uncommitted transaction
        for (version, keys) in manager.get_uncommited_txn().drain() {
            for key in keys {
                engine.del(Key::new(key, version).encode())?;
            }
        }

        let engine = Arc::new(engine);
        let manager = Arc::new(manager);

        let e = engine.clone();
        let m = manager.clone();

        thread::spawn(move || {
            while rx.recv().is_ok() {
                for (version, key) in m.pending_clean.lock().drain(..) {
                    if let Err(e) = e.del(Key::new(key, version).encode()) {
                        log::error!("Error when transaction cleanup: {e}")
                    }
                }
            }
        });

        Ok(Self {
            engine: engine.clone(),
            manager: manager.clone(),
        })
    }

    /// 关闭事务引擎
    pub fn close(&self) -> BCResult<()> {
        self.manager.sync_to_file()?;
        self.engine.close()
    }

    /// 同步事务引擎
    pub fn sync(&self) -> BCResult<()> {
        self.manager.sync_to_file()?;
        self.engine.sync()
    }

    /// 判断引擎是否为空
    pub fn is_empty(&self) -> bool {
        self.engine.is_empty()
    }

    /// 获取引擎状态
    pub fn state(&self) -> EngineState {
        self.engine.state()
    }

    /// 开启一个新的事务
    pub fn begin_transaction(&self) -> Transaction {
        Transaction::begin(self.engine.clone(), self.manager.clone())
    }

    /// 利用回调函数更新事务，将会自动提交、回滚事务
    /// ## 参数
    /// - `f`: 一个回调函数，接受一个事务引用，返回一个 BCResult
    pub fn update<F>(&self, f: F) -> BCResult<()>
    where
        F: Fn(&Transaction) -> BCResult<()>,
    {
        let txn = Transaction::begin(self.engine.clone(), self.manager.clone());

        let result = f(&txn);

        if let Err(Errors::TxnConflict) = result {
            txn.rollback()?;
        } else {
            result?;
        }

        txn.commit()
    }

    #[cfg(test)]
    pub(crate) fn get_engine(&self) -> Arc<Engine> {
        self.engine.clone()
    }
}
