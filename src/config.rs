use std::path::PathBuf;

use crate::errors::{BCResult, Errors};

/// the config for the db
pub struct Config {
    // db directory path
    pub db_path: PathBuf,
    // data file size in bytes
    pub data_file_size: usize,

    pub sync_write: bool,

    pub index_type: IndexType,
}


impl Config {
    pub fn check(&self) -> BCResult<()> {
        let path_string = self.db_path.to_str();
        if path_string.is_none() || path_string.unwrap().is_empty() {
            return Err(Errors::DirPathEmpty);
        }
        if self.data_file_size == 0 {
            return Err(Errors::DataFileSizeTooSmall(self.data_file_size));
        }
        Ok(())
    }
}

pub enum IndexType {
    BTree,
    SkipList,
}
