use std::{path::PathBuf, thread::sleep, time::Duration};

use bitcask_rs_core::{config::Config, db::Engine};
use fake::{
    faker::lorem::{en::Sentence, raw::Sentence as TSentence},
    locales::EN,
    Fake,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

fn open(temp_dir: PathBuf) -> Engine {
    let config = Config {
        file_size_threshold: 1 << 30,
        db_path: temp_dir,
        sync_write: false,
        bytes_per_sync: 0,
        index_type: bitcask_rs_core::config::IndexType::BTree,
        index_num: 1,
        start_with_mmap: false,
    };

    Engine::open(config).unwrap()
}

fn get_data_source(len: usize) -> (TSentence<EN>, TSentence<EN>) {
    let key = Sentence(63..64);
    let value = Sentence(len - 1..len);

    (key, value)
}

fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let (key, value) = get_data_source(500);

    (0..1000).into_par_iter().for_each(|_| {
        assert!(engine
            .put(key.fake::<String>(), value.fake::<String>())
            .is_ok());
    });

    println!("{:?}", engine.state());

    sleep(Duration::from_secs(3600))
}
