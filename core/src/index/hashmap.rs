use parking_lot::RwLock;

use crate::{
    data::log_record::RecordPosition,
    errors::{BCResult, Errors},
    utils::Key,
};

use super::Indexer;

#[derive(Debug, Default)]
pub struct HashMap {
    inner: RwLock<hashbrown::HashMap<Key, RecordPosition>>,
}

impl Indexer for HashMap {
    fn put(&self, key: Key, positoin: RecordPosition) -> BCResult<Option<RecordPosition>> {
        let mut map = self.inner.write();
        Ok(map.insert(key, positoin))
    }

    fn get(&self, key: &[u8]) -> Option<RecordPosition> {
        let map = self.inner.read();
        map.get(key).copied()
    }

    fn del(&self, key: &[u8]) -> BCResult<RecordPosition> {
        let mut map = self.inner.write();
        map.remove(key).ok_or(Errors::KeyNotFound)
    }

    fn exist(&self, key: &[u8]) -> bool {
        self.inner.read().contains_key(key)
    }

    fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    fn len(&self) -> usize {
        self.inner.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put() {
        let bt = HashMap::default();
        assert!(bt.put("Hello".into(), RecordPosition::new(0, 1, 5)).is_ok());
    }
    #[test]
    fn get() -> BCResult<()> {
        let bt = HashMap::default();
        bt.put("Hello".into(), RecordPosition::new(0, 1, 5))?;

        let key1: Vec<u8> = "Hello".into();
        let key2: Vec<u8> = "Hell0".into();

        assert_eq!(bt.get(&key1), Some(RecordPosition::new(0, 1, 5)));

        assert_eq!(bt.get(&key2), None);

        Ok(())
    }
    #[test]
    fn del() -> BCResult<()> {
        let bt = HashMap::default();
        bt.put("Hello".into(), RecordPosition::new(0, 1, 5))?;

        assert_eq!(bt.get(b"Hello"), Some(RecordPosition::new(0, 1, 5)));

        assert!(bt.del(b"Hello").is_ok());
        assert_eq!(bt.get(b"Hello"), None);

        assert!(bt.del(b"Hello").is_err());

        Ok(())
    }
}
