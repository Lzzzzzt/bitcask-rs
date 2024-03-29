use std::{ops::Div, path::PathBuf, time::Instant};

use bitcask_rs::{config::Config, db::DBEngine};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fake::{faker::lorem::en::Sentence, Fake};

use rayon::prelude::*;

use rand::random;

async fn open(temp_dir: PathBuf) -> DBEngine {
    let config = Config {
        file_size_threshold: 1 << 30,
        db_path: temp_dir,
        sync_write: false,
        bytes_per_sync: 0,
        index_type: bitcask_rs::config::IndexType::BTree,
        index_num: 32,
    };

    DBEngine::open(config).await.unwrap()
}

fn bench(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let engine = runtime.block_on(async { open(temp_dir.path().to_path_buf()).await });

    let key = Sentence(32..64);
    let value = Sentence((3 << 10)..(5 << 10));

    let mut insert_keys = Vec::with_capacity(100000);

    runtime.block_on(async {
        for _ in 0..100000 {
            let k = key.fake::<String>();
            assert!(engine.put(k.clone(), value.fake::<String>()).await.is_ok());
            insert_keys.push(k)
        }
    });

    c.bench_function("get", |b| {
        b.to_async(&runtime).iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| async {
                assert!(engine.get(k).await.is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("put", |b| {
        b.to_async(&runtime).iter_batched(
            || (key.fake::<String>(), value.fake::<String>()),
            |(k, v)| async {
                assert!(engine.put(k, v).await.is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("del", |b| {
        b.to_async(&runtime).iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| async {
                assert!(engine.del(k).await.is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_7_get_3_put(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let engine = runtime.block_on(async { open(temp_dir.path().to_path_buf()).await });

    let key = Sentence(32..64);
    let value = Sentence((3 << 10)..(5 << 10));

    let mut insert_keys = Vec::with_capacity(100000);

    runtime.block_on(async {
        for _ in 0..100000 {
            let k = key.fake::<String>();
            assert!(engine.put(k.clone(), value.fake::<String>()).await.is_ok());
            insert_keys.push(k)
        }
    });

    c.bench_function("7-get-3-put", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                (
                    insert_keys[random::<usize>() % 100000].clone(),
                    key.fake::<String>(),
                    value.fake::<String>(),
                )
            },
            |(rk, wk, v)| async {
                if rand::random::<f32>() < 0.3 {
                    assert!(engine.put(wk, v).await.is_ok());
                } else {
                    assert!(engine.get(rk).await.is_ok());
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_multithread(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let engine = runtime.block_on(async { open(temp_dir.path().to_path_buf()).await });

    let key = Sentence(32..64);
    let value = Sentence((3 << 10)..(5 << 10));

    let insert_keys: Vec<String> = (0..100000)
        .into_par_iter()
        .map(|_| {
            let k = key.fake::<String>();
            assert!(runtime
                .block_on(async { engine.put(k.clone(), value.fake::<String>()).await.is_ok() }));
            k
        })
        .collect();

    c.bench_function("multithread-get", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();

            insert_keys.par_iter().for_each(|k| {
                assert!(black_box(
                    runtime.block_on(engine.get(k.as_bytes())).is_ok()
                ))
            });

            start.elapsed().div(100000)
        })
    });

    c.bench_function("multithread-put", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();

            insert_keys.par_iter().take(2000).for_each(|k| {
                assert!(black_box(
                    runtime
                        .block_on(engine.put(k.clone(), value.fake::<String>()))
                        .is_ok()
                ))
            });

            start.elapsed().div(2000)
        })
    });

    c.bench_function("multithread-7get-3put", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();

            insert_keys.par_iter().take(10000).for_each(|k| {
                if random::<f32>() < 0.3 {
                    assert!(runtime
                        .block_on(engine.put(k.clone(), value.fake::<String>()))
                        .is_ok())
                } else {
                    assert!(runtime.block_on(engine.get(k.as_bytes())).is_ok())
                }
            });

            start.elapsed().div(10000)
        })
    });
}

criterion_group!(benches, bench, bench_7_get_3_put, bench_multithread);
criterion_main!(benches);
