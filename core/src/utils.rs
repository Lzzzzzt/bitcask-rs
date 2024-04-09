use std::path::{Path, PathBuf};

use crate::errors::{BCResult, Errors};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[inline(always)]
pub fn check_key_valid(key: &[u8]) -> BCResult<()> {
    if key.is_empty() {
        return Err(Errors::KeyEmpty);
    }

    Ok(())
}

#[inline(always)]
pub fn key_hash(key: &[u8], num: u8) -> usize {
    (key[0] % num) as usize
}

pub(crate) fn merge_path<P: AsRef<Path>>(p: P) -> PathBuf {
    p.as_ref().join(".merge")
}

#[cfg(test)]
pub mod tests {
    use std::path::PathBuf;

    use crate::{config::Config, db::Engine, errors::BCResult};

    pub fn open(temp_dir: PathBuf) -> BCResult<Engine> {
        let config = Config {
            file_size_threshold: 64 * 1024 * 1024,
            db_path: temp_dir,
            sync_write: false,
            bytes_per_sync: 0,
            index_type: crate::config::IndexType::BTree,
            index_num: 4,
        };

        Engine::open(config)
    }
}
