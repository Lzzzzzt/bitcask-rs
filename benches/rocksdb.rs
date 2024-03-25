use criterion::{criterion_group, criterion_main, Criterion};
use fake::{faker::lorem::en::Sentence, Fake};
use rand::random;
use rayon::prelude::*;

fn rocksdb_bench(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    let engine = rocksdb::DB::open(&opts, temp_dir.path()).unwrap();

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

    c.bench_function("rocksdb-get", |b| {
        b.iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| {
                assert!(engine.get(k.as_bytes()).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("rocksdb-put", |b| {
        b.iter_batched(
            || (key.fake::<String>(), value.fake::<String>()),
            |(k, v)| {
                assert!(engine.put(k, v).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("rocksdb-del", |b| {
        b.iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| assert!(engine.delete(k.as_bytes()).is_ok()),
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, rocksdb_bench);
criterion_main!(benches);
