use crossbeam_skiplist::SkipMap;

use crate::{
    data::log_record::RecordPosition,
    errors::{BCResult, Errors},
    utils::Key,
};

use super::Indexer;

#[derive(Debug, Default)]
pub struct SkipList {
    inner: SkipMap<Key, RecordPosition>,
}

impl Indexer for SkipList {
    fn put(&self, key: Key, positoin: RecordPosition) -> BCResult<Option<RecordPosition>> {
        if let Some(e) = self.inner.remove(&key) {
            self.inner.insert(key, positoin);
            Ok(Some(*e.value()))
        } else {
            self.inner.insert(key, positoin);
            Ok(None)
        }
    }

    fn get(&self, key: &[u8]) -> Option<RecordPosition> {
        self.inner.get(key).map(|e| *e.value())
    }

    fn del(&self, key: &[u8]) -> BCResult<RecordPosition> {
        Ok(*self.inner.remove(key).ok_or(Errors::KeyNotFound)?.value())
    }

    fn exist(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put() {
        let bt = SkipList::default();
        assert!(bt.put("Hello".into(), RecordPosition::new(0, 1, 5)).is_ok());
    }
    #[test]
    fn get() -> BCResult<()> {
        let bt = SkipList::default();
        bt.put("Hello".into(), RecordPosition::new(0, 1, 5))?;

        let key1: Vec<u8> = "Hello".into();
        let key2: Vec<u8> = "Hell0".into();

        assert_eq!(bt.get(&key1), Some(RecordPosition::new(0, 1, 5)));

        assert_eq!(bt.get(&key2), None);

        Ok(())
    }
    #[test]
    fn del() -> BCResult<()> {
        let bt = SkipList::default();
        bt.put("Hello".into(), RecordPosition::new(0, 1, 5))?;

        assert_eq!(bt.get(b"Hello"), Some(RecordPosition::new(0, 1, 5)));

        assert!(bt.del(b"Hello").is_ok());
        assert_eq!(bt.get(b"Hello"), None);

        assert!(bt.del(b"Hello").is_err());

        Ok(())
    }
}
