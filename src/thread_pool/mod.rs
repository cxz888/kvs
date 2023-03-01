mod naive;
mod rayon_wrapper;
mod shared_queue;

use crate::Result;

pub use naive::NaiveThreadPool;
pub use rayon_wrapper::RayonThreadPool;
pub use shared_queue::SharedQueueThreadPool;

/// Thread pool trait
pub trait ThreadPool: Sized {
    /// get self
    fn new(threads: u32) -> Result<Self>;
    /// spawn a task
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
