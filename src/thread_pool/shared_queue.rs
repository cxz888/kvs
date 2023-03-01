use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::mpsc::{self, Sender, SyncSender},
    thread::{self, JoinHandle},
};

use super::ThreadPool;

use crate::{Error, Result};

/// A thread pool based on shared queue
pub struct SharedQueueThreadPool {
    job_dispatcher: Sender<BoxedJob>,
}

type BoxedJob = Box<dyn FnOnce() + Send + 'static>;

struct WorkerHandle {
    join_handle: JoinHandle<()>,
    job_sender: SyncSender<BoxedJob>,
}

impl WorkerHandle {
    fn new(id: usize, waker: SyncSender<usize>) -> Self {
        let (job_sender, job_receiver) = mpsc::sync_channel::<BoxedJob>(1);
        let join_handle = thread::spawn({
            move || {
                // thread created and waiting for job
                if waker.send(id).is_err() {
                    return;
                }
                for job in job_receiver {
                    let job = AssertUnwindSafe(job);
                    // if panicked, just continue and abort the job
                    if catch_unwind(job).is_err() {
                        eprintln!(
                            "thread '{:?}' panicked and recover",
                            thread::current().name()
                        );
                    }
                    // thread pool is dropped
                    if waker.send(id).is_err() {
                        return;
                    }
                }
            }
        });
        WorkerHandle {
            join_handle,
            job_sender,
        }
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {}
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(n_threads: u32) -> Result<Self> {
        let n_threads = n_threads as usize;
        if n_threads == 0 {
            return Err(Error::ZeroSizedPool);
        }
        let (job_dispatcher, repeater) = mpsc::channel();
        // this thread(master) will return when job_dispatcher is dropped, aka thread pool is dropped
        // no need to join it, just detach
        thread::spawn(move || {
            // spawn `n_threads` threads
            let mut threads = Vec::with_capacity(n_threads);
            let (waker, sleeper) = mpsc::sync_channel(n_threads);
            for i in 0..n_threads {
                // worker will waker the master when it is idle
                let thread_handle = WorkerHandle::new(i, waker.clone());
                threads.push(thread_handle);
            }
            // receive job from user, and transmit to idle worker
            for job in repeater {
                // waker will always dropped after sleeper
                let id = sleeper.recv().unwrap();
                threads[id].job_sender.send(job).unwrap();
            }
            // worker thread's loop will be break
            drop(sleeper);
            drop(waker);
            for handle in threads {
                drop(handle.job_sender);
                handle.join_handle.join().unwrap();
            }
        });
        Ok(Self { job_dispatcher })
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.job_dispatcher.send(Box::new(job)).unwrap()
    }
}
