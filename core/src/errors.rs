use thiserror::Error;

use crate::consts::*;

#[derive(Debug, Error)]
pub enum Errors {
    // index
    #[error("Key Not Found")]
    KeyNotFound,

    // io
    #[error("Failed to read from data file: {0}")]
    ReadDataFileFaild(std::io::Error),
    #[error("Failed to write into data file: {0}")]
    WriteDataFileFaild(std::io::Error),
    #[error("Failed to sync with data file: {0}")]
    SyncDataFileFaild(std::io::Error),
    #[error("Failed to open the data file: {0}")]
    OpenDataFileFailed(std::io::Error),
    #[error("Failed to map the data file: {0}")]
    MemoryMapFileFailed(std::io::Error),
    #[error("Failed to open the lock file: {DB_FILE_LOCK}")]
    OpenLockFileFailed,
    #[error("Failed to set the file lengthe: {0}")]
    SetFileLenFailed(u32),
    #[error("Unknown log record type")]
    UnknownRecordDataType,

    // DBEngine
    #[error("Key Empty Error")]
    KeyEmpty,
    #[error("Memory index update failed")]
    MemoryIndexUpdateFailed,
    #[error("Fild not found: filename: {0:09}{DB_DATA_FILE_SUFFIX}")]
    FileNotFound(u32),
    #[error("Create DB directory failed: path is: {0}, due to: {1}")]
    CreateDBDirFailed(Box<str>, std::io::Error),
    #[error("Open DB directory failed: path is: {0}, due to: {1}")]
    OpenDBDirFailed(Box<str>, std::io::Error),
    #[error("DB data file may be damaged: filename: {0}")]
    DataFileMayBeDamaged(Box<str>),
    #[error("Read data file end of file")]
    DataFileEndOfFile,
    #[error("Invalid log record crc, data may be damaged")]
    InvalidRecordCRC,
    #[error("This directory is using")]
    DBIsInUsing,

    // Config
    #[error("DB directory path is empty")]
    DirPathEmpty,
    #[error("Data file size too small: current is {0}B")]
    DataFileSizeTooSmall(u32),

    // Batch
    #[error("Exceed the max batch size, current is: {0}")]
    ExceedMaxBatchSize(usize),

    // Merge
    #[error("DB Engine is merging")]
    DBIsMerging,
    #[error("Create DB directory failed: path is: {0}, due to: {1}")]
    CreateMergeDirFailed(Box<str>, std::io::Error),

    // Expire
    #[error("Invalid Expire Time")]
    InvalidExpireTime,
}

pub type BCResult<T> = std::result::Result<T, Errors>;
