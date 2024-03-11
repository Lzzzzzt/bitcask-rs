use std::path::PathBuf;

use bitcask_rs::{config::Config, db::DBEngine};
use criterion::{criterion_group, criterion_main, Criterion};
use fake::{faker::lorem::en::Sentence, Fake};
use rand::random;

fn open(temp_dir: PathBuf) -> DBEngine {
    let config = Config {
        file_size_threshold: 512 << 20,
        db_path: temp_dir,
        sync_write: false,
        index_type: bitcask_rs::config::IndexType::BTree,
    };

    DBEngine::open(config).unwrap()
}

fn bench_put(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(16..512);
    let value = Sentence(128..2048);

    c.bench_function("bitcask-rs-bench-put", |b| {
        b.iter_batched(
            || (key.fake::<String>(), value.fake::<String>()),
            |(k, v)| {
                assert!(engine.put(k, v).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_get(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(16..512);
    let value = Sentence(128..2048);

    let mut insert_keys = Vec::with_capacity(100000);

    for _ in 0..100000 {
        let k = key.fake::<String>();
        insert_keys.push(k.clone());
        assert!(engine.put(k, value.fake::<String>()).is_ok());
    }

    c.bench_function("bitcask-rs-bench-get", |b| {
        b.iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| {
                let res = engine.get(k.as_bytes());
                assert!(res.is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_del(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(16..512);
    let value = Sentence(128..2048);

    let mut insert_keys = Vec::with_capacity(100000);

    for _ in 0..100000 {
        let k = key.fake::<String>();
        insert_keys.push(k.clone());
        assert!(engine.put(k, value.fake::<String>()).is_ok());
    }

    c.bench_function("bitcask-rs-bench-del", |b| {
        b.iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| assert!(engine.del(k.as_bytes()).is_ok()),
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_put, bench_get, bench_del);
criterion_main!(benches);
