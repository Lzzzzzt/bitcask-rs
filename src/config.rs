use std::path::PathBuf;

use crate::errors::{BCResult, Errors};

/// the config for the db
pub struct Config {
    // db directory path
    pub db_path: PathBuf,
    // data file size in bytes
    pub file_size_threshold: u32,
    // if true, db engine will sync every write op, otherwise, just sync when file size is bigger then data file size
    pub sync_write: bool,

    pub index_type: IndexType,
}

impl Config {
    pub fn check(&self) -> BCResult<()> {
        let path_string = self.db_path.to_str();
        if path_string.is_none() || path_string.unwrap().is_empty() {
            return Err(Errors::DirPathEmpty);
        }
        if self.file_size_threshold == 0 {
            return Err(Errors::DataFileSizeTooSmall(self.file_size_threshold));
        }
        Ok(())
    }
}

pub enum IndexType {
    BTree,
    SkipList,
}

#[derive(Clone, Copy)]
pub struct WriteBatchConfig {
    pub max_bacth_size: u32,
    pub sync_write: bool,
}

impl Default for WriteBatchConfig {
    fn default() -> Self {
        Self {
            max_bacth_size: 1 << 14,
            sync_write: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Config {
        pub(crate) fn test_config(path: PathBuf) -> Self {
            Config {
                file_size_threshold: 64 * 1024 * 1024,
                db_path: path,
                sync_write: false,
                index_type: crate::config::IndexType::BTree,
            }
        }
    }
}
