use thiserror::Error;

pub type Result<T> = std::result::Result<T, Errors>;

#[derive(Error, Debug)]
pub enum Errors {
    #[error("Write File Error: {0}")]
    WriteFileFailed(std::io::Error),

    #[error("Read File Error: {0}")]
    ReadFileFailed(std::io::Error),

    #[error("Invalid CRC32")]
    InvalidCRC32,
}
