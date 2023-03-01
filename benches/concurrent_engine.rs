use std::{sync::mpsc, thread};

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::{
    rwlock,
    thread_pool::{SharedQueueThreadPool, ThreadPool},
    KvStore, KvsEngine, SledKvsEngine,
};
use tempfile::TempDir;

fn concurrent_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_set");
    group.sample_size(20);
    let n_cpus = thread::available_parallelism().unwrap().get() as u32;
    let pool = SharedQueueThreadPool::new(n_cpus * 2).unwrap();
    for tot in [100, 500, 1000, 10000] {
        group.bench_with_input(BenchmarkId::new("kvs", tot), &tot, |b, &tot| {
            b.iter_batched_ref(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let db = KvStore::open(temp_dir.path()).unwrap();
                    (db, temp_dir)
                },
                |(db, _temp_dir)| {
                    let (tx, rx) = mpsc::sync_channel(tot - 1);
                    for key_i in 1..tot {
                        let db = db.clone();
                        let tx = tx.clone();
                        pool.spawn(move || {
                            db.set(format!("key{}", key_i), "value".to_string())
                                .unwrap();
                            tx.send(()).unwrap();
                        })
                    }
                    drop(tx);
                    let mut count = 0;
                    for _ in rx {
                        count += 1;
                        if count >= tot - 1 {
                            break;
                        }
                    }
                },
                BatchSize::SmallInput,
            )
        });
        group.bench_with_input(BenchmarkId::new("kvs_rwlock", tot), &tot, |b, &tot| {
            b.iter_batched_ref(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let db = rwlock::KvStore::open(temp_dir.path()).unwrap();
                    (db, temp_dir)
                },
                |(db, _temp_dir)| {
                    let (tx, rx) = mpsc::sync_channel(tot - 1);
                    for key_i in 1..tot {
                        let db = db.clone();
                        let tx = tx.clone();
                        pool.spawn(move || {
                            db.set(format!("key{}", key_i), "value".to_string())
                                .unwrap();
                            tx.send(()).unwrap();
                        })
                    }
                    drop(tx);
                    let mut count = 0;
                    for _ in rx {
                        count += 1;
                        if count >= tot - 1 {
                            break;
                        }
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
                    let (tx, rx) = mpsc::sync_channel(tot - 1);
                    for key_i in 1..tot {
                        let db = db.clone();
                        let tx = tx.clone();
                        pool.spawn(move || {
                            db.set(format!("key{}", key_i), "value".to_string())
                                .unwrap();
                            tx.send(()).unwrap();
                        })
                    }
                    drop(tx);
                    let mut count = 0;
                    for _ in rx {
                        count += 1;
                        if count >= tot - 1 {
                            break;
                        }
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn concurrent_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_get");
    group.sample_size(20);
    let n_cpus = thread::available_parallelism().unwrap().get() as u32;
    let pool = SharedQueueThreadPool::new(n_cpus * 2).unwrap();
    const KEY_TOT: usize = 100000;
    const GET_TOT: usize = 2400;
    for tot in [6, 12, 24, 120] {
        group.bench_with_input(BenchmarkId::new("kvs", tot), &tot, |b, &tot| {
            let temp_dir = TempDir::new().unwrap();
            let store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 0..KEY_TOT {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            store.flush().unwrap();
            fastrand::seed(19260817);
            b.iter(|| {
                let (tx, rx) = mpsc::sync_channel(tot - 1);
                for _ in 0..tot {
                    let store = store.clone();
                    let tx = tx.clone();
                    pool.spawn(move || {
                        for _ in 0..GET_TOT / tot {
                            let key_i = fastrand::usize(0..KEY_TOT);
                            store.get(&format!("key{}", key_i)).unwrap();
                        }
                        tx.send(()).unwrap();
                    })
                }
                drop(tx);
                let mut count = 0;
                for _ in rx {
                    count += 1;
                    if count >= tot {
                        break;
                    }
                }
            })
        });
        group.bench_with_input(BenchmarkId::new("kvs_rwlock", tot), &tot, |b, &tot| {
            let temp_dir = TempDir::new().unwrap();
            let store = rwlock::KvStore::open(temp_dir.path()).unwrap();
            for key_i in 0..KEY_TOT {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            store.flush().unwrap();
            fastrand::seed(19260817);
            b.iter(|| {
                let (tx, rx) = mpsc::sync_channel(tot - 1);
                for _ in 0..tot {
                    let store = store.clone();
                    let tx = tx.clone();
                    pool.spawn(move || {
                        for _ in 0..GET_TOT / tot {
                            let key_i = fastrand::usize(0..KEY_TOT);
                            store.get(&format!("key{}", key_i)).unwrap();
                        }
                        tx.send(()).unwrap();
                    })
                }
                drop(tx);
                let mut count = 0;
                for _ in rx {
                    count += 1;
                    if count >= tot {
                        break;
                    }
                }
            })
        });
        group.bench_with_input(BenchmarkId::new("sled", tot), &tot, |b, &tot| {
            let temp_dir = TempDir::new().unwrap();
            let db = SledKvsEngine::open(&temp_dir).unwrap();
            for key_i in 0..KEY_TOT {
                db.set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            db.flush().unwrap();
            fastrand::seed(19260817);
            b.iter(|| {
                let (tx, rx) = mpsc::sync_channel(tot - 1);
                for _ in 0..tot {
                    let db = db.clone();
                    let tx = tx.clone();
                    pool.spawn(move || {
                        for _ in 0..GET_TOT / tot {
                            let key_i = fastrand::usize(0..KEY_TOT);
                            db.get(&format!("key{}", key_i)).unwrap();
                        }
                        tx.send(()).unwrap();
                    })
                }
                drop(tx);
                let mut count = 0;
                for _ in rx {
                    count += 1;
                    if count >= tot {
                        break;
                    }
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, concurrent_set, concurrent_get);
criterion_main!(benches);
