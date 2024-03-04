pub mod btree;

use crate::config::IndexType;
use crate::data::log_record::LogRecordPosition;
use crate::errors::BCResult;

use self::btree::BTree;

/// ## Define how to handle in-memory data
pub trait Indexer: Sync + Send {
    /// ## Map `key` and `position`
    /// Create a key-position set in in-memory data
    /// ### Return Value
    /// `Ok(())` means this function call is succuss, otherwise is failed.
    fn put(&self, key: Vec<u8>, positoin: LogRecordPosition) -> BCResult<()>;
    /// ## Query `key`
    /// Query the in-memory data with given key
    /// ### Return Value
    /// + `Some(LogRecordPosition)` means in-memory data have the key-position set
    /// + `None` means in-memory data don't have the key-position set.
    fn get(&self, key: &[u8]) -> Option<LogRecordPosition>;
    /// ## Delete `key`
    /// Delete the key-position set with given key
    /// ### Return Value
    /// + `Ok(())` means the key-position set have been removed succussfully, and the position will be return
    /// + `Err()` means the given key is not found, so that this function call is failed
    fn del(&self, key: &[u8]) -> BCResult<()>;
}

pub fn create_indexer(index_type: &IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => BTree::new(),
        IndexType::SkipList => todo!(),
    }
}
