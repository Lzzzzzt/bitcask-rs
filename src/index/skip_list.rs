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
    fn put(&self, key: Key, positoin: RecordPosition) -> BCResult<()> {
        self.inner.insert(key, positoin);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<RecordPosition> {
        self.inner.get(key).map(|e| *e.value())
    }

    fn del(&self, key: &[u8]) -> BCResult<()> {
        self.inner.remove(key).ok_or(Errors::KeyNotFound)?;
        Ok(())
    }

    fn exist(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }
}
