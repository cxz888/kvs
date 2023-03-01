use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};

// TODO: The compare here doesn't make sense now,
// because the pool just spawn the job and won't wait the job to be done

fn lightweight_job(c: &mut Criterion) {
    let mut group = c.benchmark_group("lightweight_job");
    fn dummy() -> () {}
    for i in (4..=24).step_by(4) {
        group.bench_with_input(BenchmarkId::new("rayon", i), &i, |b, i| {
            let rayon_pool = RayonThreadPool::new(*i).unwrap();
            b.iter(|| {
                for _ in 0..100 {
                    rayon_pool.spawn(dummy)
                }
            });
        });
        group.bench_with_input(BenchmarkId::new("shared_queue", i), &i, |b, i| {
            let shared_queue_pool = SharedQueueThreadPool::new(*i).unwrap();
            b.iter(|| {
                for _ in 0..100 {
                    shared_queue_pool.spawn(dummy)
                }
            });
        });
    }
    group.finish();
}

fn compute_intensive(c: &mut Criterion) {
    let mut group = c.benchmark_group("compute_intensive");
    fn decompose(mut i: usize) {
        black_box({
            let mut div = 2;
            while i != 1 {
                if i % div == 0 {
                    i /= div;
                } else {
                    div += 1;
                }
            }
        })
    }

    fastrand::seed(19260817);
    let n_jobs = 100;
    let params = (0..n_jobs)
        .map(|_| fastrand::usize(0x3f3f..0x7fff))
        .collect::<Vec<usize>>();
    for i in (4..=24).step_by(4) {
        group.bench_with_input(BenchmarkId::new("rayon", i), &i, |b, i| {
            let rayon_pool = RayonThreadPool::new(*i).unwrap();
            b.iter(|| {
                for &num in &params {
                    rayon_pool.spawn(move || decompose(num))
                }
            });
        });
        group.bench_with_input(BenchmarkId::new("shared_queue", i), &i, |b, i| {
            let shared_queue_pool = SharedQueueThreadPool::new(*i).unwrap();
            b.iter(|| {
                for &num in &params {
                    shared_queue_pool.spawn(move || decompose(num))
                }
            });
        });
    }
    group.finish();
}

// TODO: Add I/O-like benchmarks

criterion_group!(benches, lightweight_job, compute_intensive);
criterion_main!(benches);
