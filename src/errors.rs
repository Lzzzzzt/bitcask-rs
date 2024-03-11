use thiserror::Error;

use crate::DB_DATA_FILE_SUFFIX;

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
    #[error("Unknown log record type")]
    UnknownRecordType,

    // DBEngine
    #[error("Key Empty Error")]
    KeyEmpty,
    #[error("Memory index update failed")]
    MemoryIndexUpdateFailed,
    #[error("Fild not found: filename: {0:09}{DB_DATA_FILE_SUFFIX}")]
    FileNotFound(u32),
    #[error("Create DB directory failed: path is: {0}, due to: {1}")]
    CreateDBDirFailed(String, std::io::Error),
    #[error("Open DB directory failed: path is: {0}, due to: {1}")]
    OpenDBDirFailed(String, std::io::Error),
    #[error("DB data file may be damaged: filename: {0}")]
    DataFileMayBeDamaged(String),
    #[error("Read data file end of file")]
    DataFileEndOfFile,
    #[error("Invalid log record crc, data may be damaged")]
    InvalidRecordCRC,

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
    CreateMergeDirFailed(String, std::io::Error),
}

pub type BCResult<T> = std::result::Result<T, Errors>;
