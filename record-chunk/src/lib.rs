#![allow(unused, dead_code)]

use std::io::Read;
use std::{
    hash::Hasher,
    io::{Cursor, IoSlice, Result, Write},
};

const HEADER_SIZE: usize = 4 + 2 + 1;
const CHUNK_SIZE: usize = 32 << 10;
const PADDING: [u8; 6] = [0; 6];

pub mod errors;

pub type ChunkPosition = (u32, u16);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChunkType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl From<u8> for ChunkType {
    fn from(value: u8) -> Self {
        match value {
            1 => ChunkType::Full,
            2 => ChunkType::First,
            3 => ChunkType::Middle,
            4 => ChunkType::Last,
            _ => panic!("Invalid chunk type"),
        }
    }
}

pub struct ChunkWriter<W: Write> {
    writer: W,
    hasher: crc32fast::Hasher,
    current_block_offset: usize,
    chunk_size: usize,
    chunk_seq: u32,
}

impl<W: Write> ChunkWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            hasher: crc32fast::Hasher::new(),
            current_block_offset: 0,
            chunk_size: CHUNK_SIZE,
            chunk_seq: 0,
        }
    }

    pub fn with_offset(writer: W, offset: usize) -> Self {
        let mut w = Self::new(writer);
        w.current_block_offset = offset;
        w
    }

    pub fn append_record(&mut self, mut record_bytes: &[u8]) -> Result<ChunkPosition> {
        let mut first_frag = true;
        let mut res = Ok(ChunkPosition::default());

        while res.is_ok() && !record_bytes.is_empty() {
            assert!(self.chunk_size > HEADER_SIZE);

            let space_left = self.chunk_size - self.current_block_offset;

            if space_left < HEADER_SIZE {
                self.writer.write_all(&PADDING[..space_left])?;
                self.current_block_offset = 0;
                self.chunk_seq += 1;
            }

            let avail_for_data = self.chunk_size - self.current_block_offset - HEADER_SIZE;

            let data_frag_len = if record_bytes.len() < avail_for_data {
                record_bytes.len()
            } else {
                avail_for_data
            };

            if first_frag {
                res = Ok(self.get_position());
            }

            let chunk_type = if first_frag && data_frag_len == record_bytes.len() {
                ChunkType::Full
            } else if first_frag {
                ChunkType::First
            } else if data_frag_len == record_bytes.len() {
                ChunkType::Last
            } else {
                ChunkType::Middle
            };

            self.emit_record(chunk_type, record_bytes, data_frag_len)?;
            record_bytes = &record_bytes[data_frag_len..];
            first_frag = false;
        }

        res
    }

    fn get_position(&self) -> ChunkPosition {
        (self.chunk_seq, self.current_block_offset as u16)
    }

    fn emit_record(&mut self, t: ChunkType, data: &[u8], len: usize) -> Result<usize> {
        assert!(len < u16::MAX as usize);

        self.hasher.reset();
        self.hasher.write_u8(t as u8);
        self.hasher.write(data);

        let check_sum = self.hasher.finish() as u32;

        let check_sum_bytes = check_sum.to_be_bytes();
        let length_bytes = (len as u16).to_be_bytes();
        let chunk_type_bytes = [t as u8];

        let data = [
            IoSlice::new(&check_sum_bytes),
            IoSlice::new(&length_bytes),
            IoSlice::new(&chunk_type_bytes),
            IoSlice::new(data),
        ];

        let write_size = self.writer.write_vectored(&data)?;

        self.current_block_offset += write_size;

        Ok(write_size)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

pub struct ChunkReader<R: Read> {
    reader: R,
    hasher: crc32fast::Hasher,
    chunk_size: usize,
    current_block_offset: usize,
    head_sracth: [u8; HEADER_SIZE],
}

impl<R: Read> ChunkReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            hasher: crc32fast::Hasher::new(),
            chunk_size: CHUNK_SIZE,
            current_block_offset: 0,
            head_sracth: [0; HEADER_SIZE],
        }
    }

    pub fn with_offset(reader: R, offset: usize) -> Self {
        let mut r = Self::new(reader);
        r.current_block_offset = offset;
        r
    }

    pub fn read_record(&mut self, dst: &mut Vec<u8>) -> Result<usize> {
        let mut check_sum: u32;
        let mut length: u16;
        let mut chunk_type: ChunkType;
        let mut dst_offset = 0usize;

        dst.clear();

        loop {
            if self.chunk_size - self.current_block_offset < HEADER_SIZE {
                self.reader.read_exact(
                    &mut self.head_sracth[0..self.chunk_size - self.current_block_offset],
                )?;
                self.current_block_offset = 0;
            }

            let mut bytes_read = self.reader.read(&mut self.head_sracth)?;

            if bytes_read == 0 {
                return Ok(0);
            }

            self.current_block_offset += bytes_read;

            check_sum = u32::from_be_bytes([
                self.head_sracth[0],
                self.head_sracth[1],
                self.head_sracth[2],
                self.head_sracth[3],
            ]);
            length = u16::from_be_bytes([self.head_sracth[4], self.head_sracth[5]]);
            chunk_type = ChunkType::from(self.head_sracth[6]);

            dst.resize(dst_offset + length as usize, 0);
            bytes_read = self
                .reader
                .read(&mut dst[dst_offset..dst_offset + length as usize])?;

            self.current_block_offset += bytes_read;

            self.hasher.reset();

            self.hasher.write_u8(chunk_type as u8);
            self.hasher
                .write(&dst[dst_offset..dst_offset + length as usize]);

            if check_sum != self.hasher.finish() as u32 {
                panic!("Invalid CRC");
            }

            dst_offset += length as usize;

            if chunk_type == ChunkType::Full || chunk_type == ChunkType::Last {
                return Ok(dst_offset);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write() -> std::io::Result<()> {
        let mut a = ChunkWriter::new(Cursor::new(vec![]));

        let pos = a.append_record(b"111")?;

        assert_eq!(pos.0, 0);
        assert_eq!(pos.1, 0);

        a.flush()?;

        Ok(())
    }

    #[test]
    fn read() -> std::io::Result<()> {
        let mut a = ChunkWriter::new(Cursor::new(vec![]));

        a.append_record(b"111")?;
        a.flush()?;

        let mut r = ChunkReader::new(Cursor::new(a.writer.into_inner()));

        let mut buf = vec![];

        let n = r.read_record(&mut buf)?;

        assert_eq!(n, 3);
        assert_eq!(buf, b"111");

        Ok(())
    }
}
