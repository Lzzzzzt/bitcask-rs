use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::errors::*;

use super::io::IO;

/// Standard System File that implement the trait `IO`
pub struct SystemFile {
    /// System File Descriptor
    fd: File,
}

impl SystemFile {
    #[allow(unused)]
    pub fn new(filename: impl AsRef<Path>) -> BCResult<Self> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(filename)
            .map(|file| Self { fd: file })
            .map_err(Errors::OpenDataFileFailed)
    }
}

impl IO for SystemFile {
    fn write(&mut self, buf: &[u8], _: u32) -> BCResult<u32> {
        Ok(self.fd.write(buf).map_err(Errors::WriteDataFileFaild)? as u32)
    }

    fn read(&self, buf: &mut [u8], offset: u32) -> BCResult<usize> {
        self.fd
            .read_at(buf, offset as u64)
            .map_err(Errors::ReadDataFileFaild)
    }

    fn sync(&self) -> BCResult<()> {
        self.fd.sync_all().map_err(Errors::SyncDataFileFaild)
    }
}

#[cfg(test)]
impl From<File> for SystemFile {
    fn from(value: File) -> Self {
        Self { fd: value }
    }
}

#[cfg(test)]
mod tests {

    use std::io::Write;

    use crate::file::io::IO;

    use super::{BCResult, SystemFile};

    #[test]
    fn write() -> BCResult<()> {
        let path = tempfile::tempfile().unwrap();
        let mut system_file: SystemFile = path.into();

        let write_size = system_file.write("Hello".as_bytes(), 0)?;
        assert_eq!(write_size, 5);

        Ok(())
    }

    #[test]
    fn read() -> BCResult<()> {
        let mut path = tempfile::tempfile().unwrap();
        path.write_all(b"Hello World").unwrap();

        let system_file: SystemFile = path.into();
        let mut buf = [0u8; 5];

        let read_size = system_file.read(&mut buf, 0)?;
        assert_eq!(read_size, 5);
        assert_eq!(b"Hello", &buf);

        let read_size = system_file.read(&mut buf, 6)?;
        assert_eq!(read_size, 5);
        assert_eq!(b"World", &buf);

        Ok(())
    }

    #[test]
    fn sync() -> BCResult<()> {
        let path = tempfile::tempfile().unwrap();
        let mut system_file: SystemFile = path.into();

        let write_size = system_file.write("Hello".as_bytes(), 0)?;
        assert_eq!(write_size, 5);

        system_file.sync()?;

        Ok(())
    }
}
