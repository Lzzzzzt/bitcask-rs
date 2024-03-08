use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use parking_lot::RwLock;

use crate::errors::*;

use super::io::IO;

/// Standard System File that implement the trait `IO`
pub struct SystemFile {
    /// System File Descriptor
    fd: RwLock<File>,
}

impl SystemFile {
    pub fn new(filename: impl AsRef<Path>) -> BCResult<Self> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(filename)
            .map(|file| Self {
                fd: RwLock::new(file),
            })
            .map_err(Errors::OpenDataFileFailed)
    }
}

impl IO for SystemFile {
    fn write(&self, buf: &[u8]) -> BCResult<u32> {
        let mut fd = self.fd.write();
        let write_size = fd.write(buf).map_err(Errors::WriteDataFileFaild)?;
        Ok(write_size as u32)
    }

    fn read(&self, buf: &mut [u8], offset: u32) -> BCResult<usize> {
        let fd = self.fd.read();
        fd.read_at(buf, offset as u64)
            .map_err(Errors::ReadDataFileFaild)
    }

    fn sync(&self) -> BCResult<()> {
        let fd = self.fd.write();
        fd.sync_all().map_err(Errors::SyncDataFileFaild)
    }
}

impl TryFrom<File> for SystemFile {
    type Error = Errors;

    fn try_from(value: File) -> Result<Self, Self::Error> {
        Ok(Self {
            fd: RwLock::new(value),
        })
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
        let system_file: SystemFile = path.try_into()?;

        let write_size = system_file.write("Hello".as_bytes())?;
        assert_eq!(write_size, 5);

        Ok(())
    }

    #[test]
    fn read() -> BCResult<()> {
        let mut path = tempfile::tempfile().unwrap();
        path.write_all(b"Hello World").unwrap();

        let system_file: SystemFile = path.try_into()?;
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
        let system_file: SystemFile = path.try_into()?;

        let write_size = system_file.write("Hello".as_bytes())?;
        assert_eq!(write_size, 5);

        system_file.sync()?;

        Ok(())
    }
}
