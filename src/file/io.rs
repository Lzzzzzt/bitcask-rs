use std::path::Path;

use crate::errors::BCResult;

use super::system_file::SystemFile;

/// ## IO Manage
/// define different io behavior
pub trait IO: Sync + Send {
    /// write `buf` into the `io`
    /// ## Return Value
    /// + `Ok(usize)` means `usize` bytes data have been succussfully write into the io
    /// + `Err` means this function call failed
    fn write(&self, buf: &[u8]) -> BCResult<usize>;

    /// read from the `io` at the given `offset` with the `buf` length
    /// ## Return Value
    /// + `Ok(usize)` means `usize` bytes data have been succussfully read into the `buf`
    /// + `Err` means this function call failed
    fn read(&self, buf: &mut [u8], offset: usize) -> BCResult<usize>;

    /// sync data
    /// TODO: not enough
    /// ## Return Value
    /// + `Ok(())` means this function call succussed
    /// + `Err` means this function call failed
    fn sync(&self) -> BCResult<()>;
}

pub fn create_io_manager(filename: impl AsRef<Path>) -> BCResult<impl IO> {
    SystemFile::new(filename)
}
