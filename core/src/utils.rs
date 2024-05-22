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
            file_size_threshold: 64 * 1000 * 1000,
            db_path: temp_dir,
            sync_write: false,
            bytes_per_sync: 0,
            index_type: crate::config::IndexType::BTree,
            index_num: 4,
            start_with_mmap: false,
        };

        Engine::open(config)
    }

    #[test]
    fn key_hash() {
        assert_eq!(super::key_hash(&[1, 2, 3, 4], 4), 1);
        assert_eq!(super::key_hash(&[2, 3, 4, 5], 4), 2);
        assert_eq!(super::key_hash(&[3, 4, 5, 6], 4), 3);
        assert_eq!(super::key_hash(&[4, 5, 6, 7], 4), 0);
    }

    #[test]
    fn check_key_valid() {
        assert!(super::check_key_valid(&[1, 2, 3, 4]).is_ok());
        assert!(super::check_key_valid(&[]).is_err());
    }
}
