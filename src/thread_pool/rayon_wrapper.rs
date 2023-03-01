use super::ThreadPool;

use crate::Result;

/// A rayon based thread pool
pub struct RayonThreadPool {
    inner: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(RayonThreadPool {
            inner: rayon::ThreadPoolBuilder::new()
                .num_threads(threads as usize)
                .build()?,
        })
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.inner.install(job)
    }
}
