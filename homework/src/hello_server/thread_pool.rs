//! Thread pool that joins all thread when dropped.

#![allow(clippy::mutex_atomic)]

// NOTE: Crossbeam channels are MPMC, which means that you don't need to wrap the receiver in
// Arc<Mutex<..>>. Just clone the receiver and give it to each worker thread.
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::thread;
use std::{
    iter,
    sync::{Arc, Condvar, Mutex},
};
use thread::JoinHandle;

struct Job(Box<dyn FnOnce() + Send + 'static>);

#[derive(Debug)]
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Worker {
    /// When dropped, the thread's `JoinHandle` must be `join`ed.  If the worker panics, then this
    /// function should panic too.  NOTE: that the thread is detached if not `join`ed explicitly.
    fn drop(&mut self) {
        let join_handler = self.thread.take().unwrap();
        join_handler.join().unwrap();
        self.thread = None;
    }
}

/// Internal data structure for tracking the current job status. This is shared by the worker
/// closures via `Arc` so that the workers can report to the pool that it started/finished a job.
#[derive(Debug, Default)]
struct ThreadPoolInner {
    job_count: Mutex<usize>,
    empty_condvar: Condvar,
}

impl ThreadPoolInner {
    /// Increment the job count.
    fn start_job(&self) {
        let mut job_count = self.job_count.lock().unwrap();
        (*job_count) += 1;
        drop(job_count);
    }

    /// Decrement the job count.
    fn finish_job(&self) {
        let mut job_count = self.job_count.lock().unwrap();
        (*job_count) -= 1;
        drop(job_count);
    }

    /// Wait until the job count becomes 0.
    ///
    /// NOTE: We can optimize this function by adding another field to `ThreadPoolInner`, but let's
    /// not care about that in this homework.
    fn wait_empty(&self) {
        let mut job_count = self.job_count.lock().unwrap();
        let mut iter_count = 0;
        while (*job_count) != 0 {
            println!(
                "It is inside the loop and job count is: {:?}, iteration: {:?}",
                job_count, iter_count
            );
            job_count = self.empty_condvar.wait(job_count).unwrap();
            iter_count += 1;
            //job_count = self.job_count.lock().unwrap();
        }
        println!("It has finished loop");
    }
}

/// Thread pool.
#[derive(Debug)]
pub struct ThreadPool {
    workers: Vec<Worker>,
    job_sender: Option<Sender<Job>>,
    pool_inner: Arc<ThreadPoolInner>,
}

impl ThreadPool {
    /// Create a new ThreadPool with `size` threads. Panics if the size is 0.
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let (sender, receiver) = unbounded();
        let shared_data = Arc::new(ThreadPoolInner::default());
        let vec_workers = {
            let mut vec_workers = vec![];
            for i in 0..size {
                vec_workers.push(Worker {
                    id: i,
                    thread: Some(thread_spawn_pool(
                        receiver.clone(),
                        shared_data.clone(),
                        i.to_owned(),
                        size,
                    )),
                });
            }
            vec_workers
        };
        ThreadPool {
            workers: vec_workers,
            job_sender: Some(sender),
            pool_inner: shared_data,
        }
    }

    /// Execute a new job in the thread pool.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // Send the job to initiate
        self.job_sender
            .as_ref()
            .unwrap()
            .send(Job(Box::new(f)))
            .expect("Failed to initiate the job");
    }

    /// Block the current thread until all jobs in the pool have been executed.  NOTE: This method
    /// has nothing to do with `JoinHandle::join`.
    pub fn join(&self) {
        let sender = self.job_sender.as_ref().unwrap();
        while sender.len() != 0 {}
        self.pool_inner.as_ref().wait_empty();
    }
}

impl Drop for ThreadPool {
    /// When dropped, all worker threads' `JoinHandle` must be `join`ed. If the thread panicked,
    /// then this function should panic too.
    fn drop(&mut self) {
        // Drop the sender
        let sender = self.job_sender.take().unwrap();
        drop(sender);

        // Wait until all workers are done
        for worker in self.workers.iter_mut() {
            drop(worker);
        }
    }
}

fn thread_spawn_pool(
    recv: Receiver<Job>,
    shared_data: Arc<ThreadPoolInner>,
    id: usize,
    max_size: usize,
) -> JoinHandle<()> {
    let mut builder = thread::Builder::new();
    builder = builder.name(id.to_string());

    builder
        .spawn(move || {
            loop {
                let message = recv.recv();

                let job = match message {
                    Ok(Job(job)) => job,
                    // The ThreadPool was dropped.
                    Err(..) => break,
                };

                shared_data.as_ref().start_job();

                // Process the job
                job();

                shared_data.as_ref().finish_job();

                shared_data.as_ref().empty_condvar.notify_all();
            }
        })
        .unwrap()
}

#[cfg(test)]
mod thread_test {
    use super::ThreadPool;
    use crossbeam_channel::bounded;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread::sleep;
    use std::time::Duration;

    const NUM_THREADS: usize = 4;
    const NUM_JOBS: usize = 24;

    #[test]
    fn thread_pool_parallel() {
        let pool = ThreadPool::new(NUM_THREADS);
        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        let (done_sender, done_receiver) = bounded(NUM_THREADS);
        for _ in 0..NUM_THREADS {
            let barrier = barrier.clone();
            let done_sender = done_sender.clone();
            pool.execute(move || {
                barrier.wait();
                done_sender.send(()).unwrap();
            });
        }
        for _ in 0..NUM_THREADS {
            done_receiver.recv_timeout(Duration::from_secs(3)).unwrap();
        }
    }

    // Run jobs that take NUM_JOBS milliseconds as a whole.
    fn run_jobs(pool: &ThreadPool, counter: &Arc<AtomicUsize>) {
        for _ in 0..NUM_JOBS {
            let counter = counter.clone();
            pool.execute(move || {
                sleep(Duration::from_millis(NUM_THREADS as u64));
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
    }

    /// `join` blocks until all jobs are finished.
    #[test]
    fn thread_pool_join_block() {
        let pool = ThreadPool::new(NUM_THREADS);
        let counter = Arc::new(AtomicUsize::new(0));
        run_jobs(&pool, &counter);
        pool.join();
        assert_eq!(counter.load(Ordering::Relaxed), NUM_JOBS);
    }

    /// `drop` blocks until all jobs are finished.
    #[test]
    fn thread_pool_drop_block() {
        let pool = ThreadPool::new(NUM_THREADS);
        let counter = Arc::new(AtomicUsize::new(0));
        run_jobs(&pool, &counter);
        drop(pool);
        assert_eq!(counter.load(Ordering::Relaxed), NUM_JOBS);
    }

    /// This indirectly tests if the worker threads' `JoinHandle`s are joined when the pool is
    /// dropped.
    #[test]
    #[should_panic]
    fn thread_pool_drop_propagate_panic() {
        let pool = ThreadPool::new(NUM_THREADS);
        pool.execute(move || {
            panic!();
        });
    }
}
