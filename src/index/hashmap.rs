use parking_lot::RwLock;

use crate::{data::log_record::RecordPosition, utils::Key};

use super::Indexer;

#[derive(Debug, Default)]
pub struct HashMap {
    inner: RwLock<hashbrown::HashMap<Key, RecordPosition>>,
}

impl Indexer for HashMap {
    fn put(&self, key: Key, positoin: RecordPosition) -> crate::errors::BCResult<()> {
        let mut map = self.inner.write();
        map.insert(key, positoin);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<RecordPosition> {
        let map = self.inner.read();
        map.get(key).copied()
    }

    fn del(&self, key: &[u8]) -> crate::errors::BCResult<()> {
        let mut map = self.inner.write();
        map.remove(key);
        Ok(())
    }
}
