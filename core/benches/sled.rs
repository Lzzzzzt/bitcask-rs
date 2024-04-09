use criterion::{criterion_group, criterion_main, Criterion};
use fake::{faker::lorem::en::Sentence, Fake};
use rand::random;
use rayon::prelude::*;

fn sled_bench(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = sled::Config::new()
        .path(temp_dir.path())
        .temporary(true)
        .open()
        .unwrap();

    let key = Sentence(32..64);
    let value = Sentence((3 << 10)..(5 << 10));

    let insert_keys: Vec<String> = (0..100000)
        .into_par_iter()
        .map(|_| {
            let k = key.fake::<String>();
            assert!(engine
                .insert(k.clone(), value.fake::<String>().as_bytes())
                .is_ok());
            k
        })
        .collect();

    c.bench_function("sled-get", |b| {
        b.iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| {
                assert!(engine.get(k.as_bytes()).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("sled-put", |b| {
        b.iter_batched(
            || (key.fake::<String>(), value.fake::<String>()),
            |(k, v)| {
                assert!(engine.insert(k, v.as_bytes()).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("sled-del", |b| {
        b.iter_batched(
            || insert_keys[random::<usize>() % 100000].clone(),
            |k| assert!(engine.remove(k.as_bytes()).is_ok()),
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, sled_bench);
criterion_main!(benches);
