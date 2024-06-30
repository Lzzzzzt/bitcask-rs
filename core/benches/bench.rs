use std::path::PathBuf;

use bitcask_rs_core::{config::Config, db::Engine};
use criterion::{criterion_group, criterion_main, Criterion};
use fake::{faker::lorem::en::Sentence, Fake};
use rand::{random, thread_rng, Rng};
use rayon::prelude::*;

fn open(temp_dir: PathBuf) -> Engine {
    let config = Config {
        file_size_threshold: 1 << 30,
        db_path: temp_dir,
        sync_write: false,
        bytes_per_sync: 0,
        index_type: bitcask_rs_core::config::IndexType::HashMap,
        index_num: 8,
        start_with_mmap: false,
    };

    Engine::open(config).unwrap()
}

fn bench(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(32..64);
    let value = Sentence(4000..4001);

    let insert_keys: Vec<String> = (0..100000)
        .into_par_iter()
        .map(|_| {
            let k = key.fake::<String>();
            assert!(engine.put(k.clone(), value.fake::<String>()).is_ok());
            k
        })
        .collect();

    c.bench_function("get", |b| {
        b.iter_batched(
            || &insert_keys[random::<usize>() % 100000],
            |k| {
                assert!(engine.get(k).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("put", |b| {
        b.iter_batched(
            || (key.fake::<String>(), value.fake::<String>()),
            |(k, v)| {
                assert!(engine.put(k, v).is_ok());
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

#[allow(unused)]
fn bench_7_get_3_put(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = open(temp_dir.path().to_path_buf());

    let key = Sentence(32..64);
    let value = Sentence((3 << 10)..(5 << 10));

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

criterion_group!(benches, bench);
criterion_main!(benches);
