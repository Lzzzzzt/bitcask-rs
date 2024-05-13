use std::sync::Arc;
use std::thread::{self};

use crossbeam_channel::unbounded;

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
    pub fn new(engine: Engine) -> BCResult<Self> {
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

    pub fn close(&self) -> BCResult<()> {
        self.manager.sync_to_file()?;
        self.engine.close()
    }

    pub fn sync(&self) -> BCResult<()> {
        self.manager.sync_to_file()?;
        self.engine.sync()
    }

    pub fn is_empty(&self) -> bool {
        self.engine.is_empty()
    }

    pub fn state(&self) -> EngineState {
        self.engine.state()
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction::begin(self.engine.clone(), self.manager.clone())
    }

    pub fn update<F>(&self, f: F) -> BCResult<()>
    where
        F: Fn(&Transaction) -> BCResult<()>,
    {
        let txn = Transaction::begin(self.engine.clone(), self.manager.clone());

        f(&txn)?;

        txn.commit()
    }

    #[cfg(test)]
    pub fn get_engine(&self) -> Arc<Engine> {
        self.engine.clone()
    }
}
