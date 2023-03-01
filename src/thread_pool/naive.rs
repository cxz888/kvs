use std::{
    cell::RefCell,
    thread::{self, JoinHandle},
};

use super::ThreadPool;

use crate::Result;

/// A naive thread pool, do not reuse, just spawn new thread
pub struct NaiveThreadPool {
    handles: RefCell<Vec<JoinHandle<()>>>,
}

impl Drop for NaiveThreadPool {
    fn drop(&mut self) {
        let handles = std::mem::take(self.handles.get_mut());
        for handle in handles {
            _ = handle.join();
        }
    }
}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(Self {
            handles: RefCell::new(Vec::with_capacity(threads as usize)),
        })
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.handles.borrow_mut().push(thread::spawn(job))
    }
}
