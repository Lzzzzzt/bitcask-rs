use std::{path::PathBuf, sync::Arc, thread, time::Instant};

use bitcask_rs::{config::Config, db::DBEngine};
use fake::{faker::lorem::en::Sentence, Fake};

fn open(temp_dir: PathBuf) -> DBEngine {
    let config = Config {
        file_size_threshold: 1 << 30,
        db_path: temp_dir,
        sync_write: false,
        bytes_per_sync: 0,
        index_type: bitcask_rs::config::IndexType::BTree,
        index_num: 32,
    };

    DBEngine::open(config).unwrap()
}

fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open(temp_dir.path().to_path_buf()));

    let mut handlers = vec![];

    for _ in 0..32 {
        let eng = Arc::clone(&engine);

        handlers.push(thread::spawn(move || {
            let key = Sentence(32..64);
            let value = Sentence((3 << 10)..(5 << 10));
            let mut costs = vec![];

            for _ in 0..2000 {
                let k = key.fake::<String>();
                let v = value.fake::<String>();
                let start = Instant::now();
                assert!(eng.put(k, v).is_ok());
                costs.push(start.elapsed().as_nanos());
            }
            costs
        }));
    }

    let mut costs = vec![];
    for handler in handlers {
        costs.extend(handler.join().unwrap());
    }

    costs.sort_unstable();

    println!("min: {}ns", costs.first().unwrap());
    println!(
        "avg: {}ns",
        costs.iter().sum::<u128>() / costs.len() as u128
    );
    println!("max: {}ns", costs.last().unwrap());
}
