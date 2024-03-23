use std::{path::PathBuf, time::Instant};

use bitcask_rs::{config::Config, db::DBEngine};
use criterion::{criterion_group, criterion_main, Criterion};
use fake::{faker::lorem::en::Sentence, Fake};
use rand::{random, thread_rng, Rng};
use rayon::prelude::*;

fn open(temp_dir: PathBuf) -> DBEngine {
    let config = Config {
        file_size_threshold: 512 << 20,
        db_path: temp_dir,
        sync_write: false,
        index_type: bitcask_rs::config::IndexType::BTree,
        index_num: 32,
    };

    DBEngine::open(config).unwrap()
}

fn bench(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(16..512);
    let value = Sentence((1 << 10)..(15 << 10));

    let mut insert_keys = Vec::with_capacity(100000);

    c.bench_function("put", |b| {
        b.iter_batched(
            || {
                let k = key.fake::<String>();
                insert_keys.push(k.clone());
                (k, value.fake::<String>())
            },
            |(k, v)| {
                assert!(engine.put(k, v).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    let par_insert_keys: Vec<String> = (insert_keys.len()..100000)
        .into_par_iter()
        .map(|_| {
            let k = key.fake::<String>();
            assert!(engine.put(k.clone(), value.fake::<String>()).is_ok());
            k
        })
        .collect();

    insert_keys.extend(par_insert_keys);

    c.bench_function("get", |b| {
        b.iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| {
                assert!(engine.get(k.as_bytes()).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("del", |b| {
        b.iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| assert!(engine.del(k.as_bytes()).is_ok()),
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_7_get_3_put(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(16..512);
    let value = Sentence((1 << 10)..(15 << 10));

    let insert_keys: Vec<String> = (0..100000)
        .into_par_iter()
        .map(|_| {
            let k = key.fake::<String>();
            assert!(engine.put(k.clone(), value.fake::<String>()).is_ok());
            k
        })
        .collect();

    let mut rng = thread_rng();

    c.bench_function("7-get-3-put", |b| {
        b.iter_batched(
            || {
                (
                    insert_keys[random::<usize>() % 100000].clone(),
                    key.fake::<String>(),
                    value.fake::<String>(),
                    rng.gen_range(0u8..10),
                )
            },
            |(rk, wk, v, rw)| {
                if rw < 3 {
                    assert!(engine.put(wk, v).is_ok());
                } else {
                    assert!(engine.get(rk.as_bytes()).is_ok());
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_multithread(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(16..512);
    let value = Sentence((1 << 10)..(15 << 10));

    let insert_keys: Vec<String> = (0..100000)
        .into_par_iter()
        .map(|_| {
            let k = key.fake::<String>();
            assert!(engine.put(k.clone(), value.fake::<String>()).is_ok());
            k
        })
        .collect();

    c.bench_function("multithread-put", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();

            insert_keys
                .par_iter()
                .take(5000)
                .for_each(|k| assert!(engine.put(k.clone(), value.fake::<String>()).is_ok()));

            start.elapsed()
        })
    });

    c.bench_function("multithread-get", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();

            insert_keys
                .par_iter()
                .for_each(|k| assert!(engine.get(k.as_bytes()).is_ok()));

            start.elapsed()
        })
    });
}

criterion_group!(benches, bench, bench_7_get_3_put, bench_multithread);
criterion_main!(benches);
