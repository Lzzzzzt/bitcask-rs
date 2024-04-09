pub mod btree;
pub mod hashmap;
pub mod skip_list;

use crate::config::IndexType;
use crate::data::log_record::RecordPosition;
use crate::errors::BCResult;
use crate::utils::Key;

use self::btree::BTree;
use self::hashmap::HashMap;
use self::skip_list::SkipList;

/// ## Define how to handle in-memory data
pub trait Indexer: Sync + Send {
    /// ## Map `key` and `position`
    /// Create a key-position set in in-memory data
    /// ### Return Value
    /// `Ok(())` means this function call is succuss, otherwise is failed.
    fn put(&self, key: Key, positoin: RecordPosition) -> BCResult<()>;
    /// ## Query `key`
    /// Query the in-memory data with given key
    /// ### Return Value
    /// + `Some(LogRecordPosition)` means in-memory data have the key-position set
    /// + `None` means in-memory data don't have the key-position set.
    fn get(&self, key: &[u8]) -> Option<RecordPosition>;
    /// ## Delete `key`
    /// Delete the key-position set with given key
    /// ### Return Value
    /// + `Ok(())` means the key-position set have been removed succussfully, and the position will be return
    /// + `Err()` means the given key is not found, so that this function call is failed
    fn del(&self, key: &[u8]) -> BCResult<()>;

    fn exist(&self, key: &[u8]) -> bool;
}

pub fn create_indexer(index_type: &IndexType, index_num: u8) -> Vec<Box<dyn Indexer>> {
    (0..index_num)
        .map(|_| {
            let index: Box<dyn Indexer> = match index_type {
                IndexType::BTree => Box::<BTree>::default(),
                IndexType::SkipList => Box::<SkipList>::default(),
                IndexType::HashMap => Box::<HashMap>::default(),
            };
            index
        })
        .collect()
}
