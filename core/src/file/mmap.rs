use std::{fs::File, path::Path};

use memmap2::{Advice, Mmap, MmapOptions};

use crate::errors::{BCResult, Errors};

use super::io::IO;

pub struct MemoryMappedFile {
    data: Mmap,
}

impl MemoryMappedFile {
    pub fn new(filename: impl AsRef<Path>) -> BCResult<Self> {
        let file = File::open(filename).map_err(Errors::OpenDataFileFailed)?;

        let mapped = unsafe {
            MmapOptions::new()
                .huge(Some(22))
                .populate()
                .map(&file)
                .map_err(Errors::MemoryMapFileFailed)?
        };

        mapped
            .advise(Advice::Sequential)
            .map_err(Errors::MemoryMapFileFailed)?;

        Ok(Self { data: mapped })
    }
}

impl IO for MemoryMappedFile {
    fn write(&mut self, _: &[u8], _: u32) -> crate::errors::BCResult<u32> {
        unimplemented!()
    }

    fn read(&self, buf: &mut [u8], offset: u32) -> crate::errors::BCResult<u32> {
        let start = offset as usize;
        let end = start + buf.len();

        assert!(end <= self.data.len());

        buf.copy_from_slice(&self.data[start..end]);
        Ok(buf.len() as u32)
    }

    fn sync(&self) -> crate::errors::BCResult<()> {
        unimplemented!()
    }
}

#[cfg(test)]
impl TryFrom<File> for MemoryMappedFile {
    type Error = Errors;

    fn try_from(value: File) -> Result<Self, Self::Error> {
        let data = unsafe {
            MmapOptions::new()
                .huge(Some(22))
                .populate()
                .map(&value)
                .map_err(Errors::MemoryMapFileFailed)?
        };

        Ok(Self { data })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn read() -> BCResult<()> {
        let mut path = tempfile::tempfile().unwrap();
        path.write_all(b"Hello World").unwrap();

        let file: MemoryMappedFile = path.try_into()?;
        let mut buf = [0u8; 5];

        let read_size = file.read(&mut buf, 0)?;
        assert_eq!(read_size, 5);
        assert_eq!(b"Hello", &buf);

        let read_size = file.read(&mut buf, 6)?;
        assert_eq!(read_size, 5);
        assert_eq!(b"World", &buf);

        Ok(())
    }
}
