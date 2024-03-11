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

#[cfg(test)]
pub mod tests {
    use std::path::PathBuf;

    use crate::{config::Config, db::DBEngine};

    pub fn open(temp_dir: PathBuf) -> DBEngine {
        let config = Config {
            file_size_threshold: 64 * 1024 * 1024,
            db_path: temp_dir,
            sync_write: false,
            index_type: crate::config::IndexType::BTree,
        };

        DBEngine::open(config).unwrap()
    }
}
