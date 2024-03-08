use std::path::PathBuf;

use bitcask_rs::{config::Config, db::DBEngine};
use criterion::{criterion_group, criterion_main, Criterion};
use fake::{faker::lorem::en::Sentence, Fake};

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
        b.iter(|| {
            assert!(engine
                .put(key.fake::<String>().into(), value.fake::<String>().into())
                .is_ok());
        })
    });
}

fn bench_get(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(16..512);
    let value = Sentence(128..2048);

    for _ in 0..100000 {
        assert!(engine
            .put(key.fake::<String>().into(), value.fake::<String>().into())
            .is_ok());
    }

    c.bench_function("bitcask-rs-bench-get", |b| {
        b.iter(|| {
            let _ = engine.get(key.fake::<String>().as_bytes());
        })
    });
}

fn bench_del(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(16..512);
    let value = Sentence(128..2048);

    for _ in 0..100000 {
        assert!(engine
            .put(key.fake::<String>().into(), value.fake::<String>().into())
            .is_ok());
    }

    c.bench_function("bitcask-rs-bench-del", |b| {
        b.iter(|| assert!(engine.del(key.fake::<String>().as_bytes()).is_ok()))
    });
}

criterion_group!(benches, bench_put, bench_get, bench_del);
criterion_main!(benches);
