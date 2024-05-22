use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
};

use crossbeam_channel::Sender;
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};

use crate::{
    config::Config,
    consts::TXN_INFO_FILE,
    errors::{BCResult, Errors},
};

pub(crate) struct TxnManager {
    version: AtomicU64,
    active_txn: Mutex<HashMap<u64, Vec<Vec<u8>>>>,
    config: Config,

    pub(crate) pending_clean: Mutex<Vec<(u64, Vec<u8>)>>,
    pub(crate) cleanup_signal: Sender<()>,
}

impl TxnManager {
    pub(crate) fn from_file_or_init(config: Config, signal: Sender<()>) -> BCResult<Self> {
        let path = config.db_path.join(TXN_INFO_FILE);

        let manager = match std::fs::read(&path) {
            Ok(info) => {
                let (active, verison): (HashMap<u64, Vec<Vec<u8>>>, u64) =
                    bincode::deserialize(&info).map_err(Errors::TxnInfoReadFailed)?;

                TxnManager {
                    config,
                    version: AtomicU64::from(verison),
                    active_txn: Mutex::new(active),
                    pending_clean: Default::default(),
                    cleanup_signal: signal,
                }
            }
            Err(e) => {
                log::error!("Open Transaction Info File Failed: {e}");
                log::warn!("Will RESET Transacion Info File");

                std::fs::File::create(path).map_err(Errors::TxnInfoCreateFailed)?;

                TxnManager {
                    config,
                    version: Default::default(),
                    active_txn: Default::default(),
                    pending_clean: Default::default(),
                    cleanup_signal: signal,
                }
            }
        };

        Ok(manager)
    }

    pub(crate) fn get_uncommited_txn(&self) -> MappedMutexGuard<HashMap<u64, Vec<Vec<u8>>>> {
        MutexGuard::map(self.active_txn.lock(), |txns| txns)
    }

    pub(crate) fn acquire_next_version(&self) -> u64 {
        self.version.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn add_txn(&self, version: u64) -> HashSet<u64> {
        let mut active = self.active_txn.lock();
        let active_txn_id = active.keys().copied().collect();
        active.insert(version, vec![]);

        active_txn_id
    }

    pub(crate) fn remove_txn(&self, version: u64) -> Option<Vec<Vec<u8>>> {
        let mut active = self.active_txn.lock();

        let res = active.remove(&version);

        if active.is_empty() {
            self.cleanup_signal.send(()).unwrap();
        }

        res
    }

    pub(crate) fn update_txn(&self, version: u64, key: &[u8]) {
        self.active_txn
            .lock()
            .entry(version)
            .and_modify(|keys| keys.push(key.to_vec()))
            .or_insert_with(|| vec![key.to_vec()]);
    }

    pub(crate) fn sync_to_file(&self) -> BCResult<()> {
        let bytes = bincode::serialize(&(
            &*self.active_txn.lock(),
            self.version.load(Ordering::SeqCst),
        ))
        .unwrap();

        let path = self.config.db_path.join(TXN_INFO_FILE);

        std::fs::write(path, bytes).map_err(Errors::TxnInfoWriteFailed)
    }

    pub(crate) fn mark_to_clean(&self, version: u64, key: Vec<u8>) {
        self.pending_clean.lock().push((version, key));
    }
}
