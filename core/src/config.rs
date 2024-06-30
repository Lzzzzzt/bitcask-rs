use std::path::PathBuf;

use crate::errors::{BCResult, Errors};

#[derive(Debug, Clone)]
/// 数据库配置
pub struct Config {
    /// 数据库路径
    pub db_path: PathBuf,
    /// 数据文件大小阈值
    pub file_size_threshold: u32,
    /// 是否同步写
    pub sync_write: bool,
    /// 每多少字节同步一次
    pub bytes_per_sync: usize,
    /// 索引类型
    pub index_type: IndexType,
    /// 索引数量
    pub index_num: u8,
    /// 是否使用 mmap 启动
    pub start_with_mmap: bool,
}

impl Config {
    /// 检查配置是否合法
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
/// 索引类型
pub enum IndexType {
    /// BTree 索引
    BTree,
    /// SkipList 索引
    SkipList,
    /// HashMap 索引
    HashMap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IOType {
    System,
    MemoryMap,
}

#[derive(Clone, Copy)]
/// 写批次配置
pub struct WriteBatchConfig {
    /// 最大批次大小
    pub max_bacth_size: u32,
    /// 是否同步写
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
