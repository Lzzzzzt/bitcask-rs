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
