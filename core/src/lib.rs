#![feature(iterator_try_collect)]

mod batch;
mod data;
mod index;
mod merge;
mod utils;

pub mod config;
pub mod db;
pub mod errors;
pub mod file;

pub(crate) mod consts {
    pub(crate) const DB_DATA_FILE_SUFFIX: &str = "bcdata";
    pub(crate) const DB_HINT_FILE: &str = "index-hint.bcdata";
    pub(crate) const DB_MERGE_FIN_FILE: &str = "merge-finished.bcdata";
    pub(crate) const DB_FILE_LOCK: &str = ".bclock";
}
