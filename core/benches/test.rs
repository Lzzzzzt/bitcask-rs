use std::{path::PathBuf, time::Instant};

use bitcask_rs_core::{config::Config, db::Engine};
use fake::{
    faker::lorem::{en::Sentence, raw::Sentence as TSentence},
    locales::EN,
    Fake,
};
use rand::seq::SliceRandom;
use rand::thread_rng;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

fn open(temp_dir: PathBuf) -> Engine {
    let config = Config {
        file_size_threshold: 1 << 30,
        db_path: temp_dir,
        sync_write: false,
        bytes_per_sync: 0,
        index_type: bitcask_rs_core::config::IndexType::HashMap,
        index_num: 32,
        start_with_mmap: false,
    };

    Engine::open(config).unwrap()
}

fn get_data_source(len: usize) -> (TSentence<EN>, TSentence<EN>) {
    let key = Sentence(63..64);
    let value = Sentence(len - 1..len);

    (key, value)
}

fn bench(size: usize) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let (key, value) = get_data_source(size);

    let mut insert_keys: Vec<String> = (0..1000000)
        .into_par_iter()
        .map(|_| {
            let k = key.fake::<String>();
            assert!(engine.put(k.clone(), value.fake::<String>()).is_ok());
            k
        })
        .collect();

    let mut rng = thread_rng();

    insert_keys.shuffle(&mut rng);

    let start = Instant::now();

    insert_keys.par_iter().for_each(|k| {
        assert!(engine.get(k).is_ok());
    });

    println!("{size}: {:?}", start.elapsed() / 1000000);
}

fn main() {
    for size in [500, 4000] {
        bench(size);
    }
}
