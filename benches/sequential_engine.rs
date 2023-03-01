use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use tempfile::TempDir;

fn sequential_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_set");
    group.sample_size(20);
    for tot in [100, 500, 1000, 10000] {
        group.bench_with_input(BenchmarkId::new("kvs", tot), &tot, |b, &tot| {
            b.iter_batched_ref(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let db = KvStore::open(temp_dir.path()).unwrap();
                    (db, temp_dir)
                },
                |(db, _temp_dir)| {
                    for key_i in 1..tot {
                        db.set(format!("key{}", key_i), "value".to_string())
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        });
        group.bench_with_input(BenchmarkId::new("sled", tot), &tot, |b, &tot| {
            b.iter_batched_ref(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let db = SledKvsEngine::open(temp_dir.path()).unwrap();
                    (db, temp_dir)
                },
                |(db, _temp_dir)| {
                    for key_i in 1..tot {
                        db.set(format!("key{}", key_i), "value".to_string())
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn sequential_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_get");
    for tot in [1000, 3000, 10000] {
        group.bench_with_input(BenchmarkId::new("kvs", tot), &tot, |b, &tot| {
            let temp_dir = TempDir::new().unwrap();
            let store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..tot {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            fastrand::seed(19260817);
            b.iter(|| {
                store
                    .get(&format!("key{}", fastrand::usize(1..tot)))
                    .unwrap();
            })
        });
        group.bench_with_input(BenchmarkId::new("sled", tot), &tot, |b, &tot| {
            let temp_dir = TempDir::new().unwrap();
            let db = SledKvsEngine::open(&temp_dir).unwrap();
            for key_i in 1..tot {
                db.set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            fastrand::seed(19260817);
            b.iter(|| {
                db.get(&format!("key{}", fastrand::usize(1..tot))).unwrap();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, sequential_set, sequential_get);
criterion_main!(benches);
