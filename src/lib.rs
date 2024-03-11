#![feature(iterator_try_collect)]

mod batch;
mod data;
mod errors;
mod index;
mod merge;
mod utils;

pub mod config;
pub mod db;
pub mod file;

pub(crate) const DB_DATA_FILE_SUFFIX: &str = "bcdata";
pub(crate) const DB_HINT_FILE: &str = "index-hint.bcdata";
pub(crate) const DB_MERGE_FIN_FILE: &str = "merge-finished.bcdata";
