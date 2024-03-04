use crate::{data::log_record::LogRecordPosition, errors::*};
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

use super::Indexer;

/// ## in-memory data using `BTreeMap`
pub struct BTree {
    inner: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPosition>>>,
}

impl BTree {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, positoin: LogRecordPosition) -> BCResult<()> {
        let mut map = self.inner.write();
        map.insert(key, positoin);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<LogRecordPosition> {
        let map = self.inner.read();
        map.get(key).copied()
    }

    fn del(&self, key: &[u8]) -> BCResult<()> {
        let mut map = self.inner.write();
        map.remove(key).ok_or(Errors::KeyNotFound)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put() {
        let bt = BTree::new();
        assert!(bt.put("Hello".into(), LogRecordPosition::new(0, 1)).is_ok());
    }
    #[test]
    fn get() -> BCResult<()> {
        let bt = BTree::new();
        bt.put("Hello".into(), LogRecordPosition::new(0, 1))?;

        let key1: Vec<u8> = "Hello".into();
        let key2: Vec<u8> = "Hell0".into();

        assert_eq!(bt.get(&key1), Some(LogRecordPosition::new(0, 1)));

        assert_eq!(bt.get(&key2), None);

        Ok(())
    }
    #[test]
    fn del() -> BCResult<()> {
        let bt = BTree::new();
        bt.put("Hello".into(), LogRecordPosition::new(0, 1))?;

        assert_eq!(bt.get(b"Hello"), Some(LogRecordPosition::new(0, 1)));

        assert!(bt.del(b"Hello").is_ok());
        assert_eq!(bt.get(b"Hello"), None);

        assert!(bt.del(b"Hello").is_err());

        Ok(())
    }
}
