use std::path::PathBuf;

use crate::errors::{BCResult, Errors};

/// the config for the db
#[derive(Debug, Clone)]
pub struct Config {
    // db directory path
    pub db_path: PathBuf,
    // data file size in bytes
    pub file_size_threshold: u32,
    // if true, db engine will sync every write op, otherwise, just sync when file size is bigger then data file size
    pub sync_write: bool,

    pub bytes_per_sync: usize,

    pub index_type: IndexType,

    pub index_num: u8,

    pub start_with_mmap: bool,
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

impl Default for Config {
    fn default() -> Self {
        Self {
            db_path: "/tmp/bitcask_rs".into(),
            file_size_threshold: 256 << 10,
            sync_write: false,
            bytes_per_sync: 0,
            index_type: IndexType::SkipList,
            index_num: 8,
            start_with_mmap: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    SkipList,
    HashMap,
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
                bytes_per_sync: 0,
                index_type: crate::config::IndexType::BTree,
                index_num: 4,
                start_with_mmap: false,
            }
        }
    }

    #[test]
    fn test_config_check() {
        let config = Config {
            file_size_threshold: 0,
            db_path: "/tmp/bitcask_rs".into(),
            sync_write: false,
            bytes_per_sync: 0,
            index_type: IndexType::SkipList,
            index_num: 8,
            start_with_mmap: false,
        };

        assert!(matches!(
            config.check().unwrap_err(),
            Errors::DataFileSizeTooSmall(0)
        ));

        let config = Config {
            file_size_threshold: 256 << 10,
            db_path: "".into(),
            sync_write: false,
            bytes_per_sync: 0,
            index_type: IndexType::SkipList,
            index_num: 8,
            start_with_mmap: false,
        };

        assert!(matches!(config.check().unwrap_err(), Errors::DirPathEmpty));

        let config = Config {
            file_size_threshold: 256 << 10,
            db_path: "/tmp/bitcask_rs".into(),
            sync_write: false,
            bytes_per_sync: 0,
            index_type: IndexType::SkipList,
            index_num: 8,
            start_with_mmap: false,
        };

        assert!(config.check().is_ok());

        let config = Config::default();

        assert!(config.check().is_ok());
    }
}
