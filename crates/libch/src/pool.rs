//! Workers started ahead of demand, and a bound on how many operations run at once.
//!
//! A worker's largest fixed cost is starting the engine. A [`Pool`] keeps workers started and waiting for a
//! request, so an operation pays for its own work alone. Each worker serves one operation and exits, and the
//! pool starts another in its place. A waiting worker has seen no request, so it holds nothing of any tenant.
//!
//! With a [forking](WorkerCommand::forking) command the pool's workers are parents that started the engine
//! once: each serves an operation in a child it forks, and comes back to the pool when the child is done. An
//! operation that finds no parent waiting starts one, which joins the pool while the pool holds fewer than
//! [`PoolOptions::waiting`]. The parent leaves each request on its pipe for the child to read, so it holds
//! nothing of any tenant either, however many operations it has served.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use crate::proto as pb;
use bytes::Bytes;
use tokio::sync::{Notify, Semaphore};

use crate::observe::{Failure, Observer};
use crate::worker::{Failed, Observing, OpWriter, Run, Worker, WorkerCommand};
use crate::{Error, Tenant, TenantKey};

/// How long the pool waits before it tries again to start a worker, at first and at most.
const FIRST_RETRY: Duration = Duration::from_millis(50);
const LAST_RETRY: Duration = Duration::from_secs(5);

/// A pool's size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolOptions {
    /// How many workers the pool keeps started and waiting for a request. Each holds the engine's idle memory.
    /// With zero, every operation starts its own worker. Forking workers serve one operation at a time each, so
    /// a pool of them serves `concurrency` operations at their lowest cost when it keeps as many waiting.
    pub waiting: usize,
    /// How many operations run at a time. Further calls wait their turn, first come first served.
    pub concurrency: usize,
}

/// What a pool holds now, and what it has done.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Workers started and waiting for a request.
    pub waiting: usize,
    /// Operations running.
    pub running: usize,
    /// The most operations that ran at one time.
    pub most_running: usize,
    /// Workers started since the pool was made.
    pub started: u64,
    /// Attempts to start a worker that failed.
    pub failed_starts: u64,
    /// Operations that found no worker waiting and started their own.
    pub unpooled: u64,
    /// Waiting workers that had exited by the time an operation came for them.
    pub lost: u64,
}

/// Runs operations in workers started ahead of demand. Clones share one pool.
///
/// The pool's waiting workers are killed when its last clone, and the last writer made from it, is dropped.
#[derive(Clone)]
pub struct Pool {
    shared: Arc<Shared>,
}

struct Shared {
    command: WorkerCommand,
    options: PoolOptions,
    observer: Observing,
    turns: Semaphore,
    state: Mutex<State>,
    /// Wakes the task that keeps the waiting workers topped up.
    wake: Arc<Notify>,
    running: AtomicUsize,
    most_running: AtomicUsize,
    started: AtomicU64,
    failed_starts: AtomicU64,
    unpooled: AtomicU64,
    lost: AtomicU64,
}

#[derive(Default)]
struct State {
    /// The workers waiting for a request, the one started first at the front.
    waiting: VecDeque<Worker>,
    /// Forking workers taken from the queue that are serving an operation.
    lent: usize,
    closed: bool,
}

impl Pool {
    /// A pool of `command` workers. The caller is inside a Tokio runtime, which runs the task that starts them.
    pub fn new(command: WorkerCommand, options: PoolOptions) -> Result<Pool, Error> {
        Pool::start(command, options, Observing::default())
    }

    /// A pool whose writers report each operation to `observer` as it ends.
    pub fn with_observer(command: WorkerCommand, options: PoolOptions, observer: Arc<dyn Observer>) -> Result<Pool, Error> {
        Pool::start(command, options, Observing(Some(observer)))
    }

    fn start(command: WorkerCommand, options: PoolOptions, observer: Observing) -> Result<Pool, Error> {
        if options.concurrency == 0 {
            return Err(Error::Other("a pool runs at least one operation at a time".into()));
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::Other("a pool is made inside a Tokio runtime".into()))?;
        let wake = Arc::new(Notify::new());
        let shared = Arc::new(Shared {
            command,
            options,
            observer,
            turns: Semaphore::new(options.concurrency),
            state: Mutex::new(State::default()),
            wake: wake.clone(),
            running: AtomicUsize::new(0),
            most_running: AtomicUsize::new(0),
            started: AtomicU64::new(0),
            failed_starts: AtomicU64::new(0),
            unpooled: AtomicU64::new(0),
            lost: AtomicU64::new(0),
        });
        runtime.spawn(top_up(Arc::downgrade(&shared), wake));
        Ok(Pool { shared })
    }

    /// A writer for one tenant whose operations run in this pool's workers. `key` holds the `write` grant on
    /// the tenant's directory.
    pub fn writer(&self, tenant: &Tenant, key: impl Into<TenantKey>) -> OpWriter {
        OpWriter::pooled(self.clone(), tenant, key.into())
    }

    pub fn options(&self) -> &PoolOptions {
        &self.shared.options
    }

    pub(crate) fn observer(&self) -> Observing {
        self.shared.observer.clone()
    }

    pub fn stats(&self) -> PoolStats {
        let shared = &self.shared;
        PoolStats {
            waiting: shared.state().waiting.len(),
            running: shared.running.load(Ordering::Relaxed),
            most_running: shared.most_running.load(Ordering::Relaxed),
            started: shared.started.load(Ordering::Relaxed),
            failed_starts: shared.failed_starts.load(Ordering::Relaxed),
            unpooled: shared.unpooled.load(Ordering::Relaxed),
            lost: shared.lost.load(Ordering::Relaxed),
        }
    }

    /// Kills the waiting workers and refuses further operations. Operations already running finish.
    pub async fn close(&self) {
        let waiting = {
            let mut state = self.shared.state();
            state.closed = true;
            std::mem::take(&mut state.waiting)
        };
        self.shared.turns.close();
        self.shared.wake.notify_one();
        for worker in waiting {
            worker.kill().await;
        }
    }

    /// Runs one operation: waits for a turn, takes a worker, and gives it the request.
    pub(crate) async fn run(&self, request: &pb::Request, data: Option<&Bytes>, deadline: Duration) -> Run {
        let shared = &self.shared;
        let queuing = Instant::now();
        let turn = shared.turns.acquire().await;
        let queued = queuing.elapsed();
        let failed = |failure, error| Run { response: Err(Failed::new(failure, error)), queued, waiting_worker: false };
        let Ok(_turn) = turn else { return failed(Failure::Closed, closed()) };
        let _running = Running::enter(shared);
        let forking = shared.command.forking;
        // A forking worker the pool lent out counts toward the pool's number until it is back or gone, so the
        // pool starts none in its place while it serves.
        let mut lent = Lent { shared, counted: false };
        let mut waiting_worker = true;
        let mut worker = loop {
            let waiting = {
                let mut state = shared.state();
                if state.closed {
                    return failed(Failure::Closed, closed());
                }
                let waiting = state.waiting.pop_front();
                if forking && waiting.is_some() {
                    state.lent += 1;
                    lent.counted = true;
                }
                waiting
            };
            let Some(mut worker) = waiting else {
                shared.unpooled.fetch_add(1, Ordering::Relaxed);
                waiting_worker = false;
                match shared.start() {
                    Ok(worker) => break worker,
                    Err(error) => return failed(Failure::Start, error),
                }
            };
            // A worker that serves one operation is used up when taken, and the pool starts another.
            if !forking {
                shared.wake.notify_one();
            }
            if !worker.has_exited() {
                break worker;
            }
            shared.lost.fetch_add(1, Ordering::Relaxed);
            lent.give_up();
        };
        let ran = |response| Run { response, queued, waiting_worker };
        if !forking {
            return ran(worker.run(request, data, deadline).await);
        }

        let (response, reusable) = worker.serve(request, data, deadline).await;
        if reusable {
            let mut state = shared.state();
            if lent.counted {
                state.lent -= 1;
                lent.counted = false;
            }
            if !state.closed && state.waiting.len() + state.lent < shared.options.waiting {
                state.waiting.push_back(worker);
                return ran(response);
            }
        }
        // The worker is dropped, which kills it and the child it may still have, and the pool starts another
        // when it is one short.
        drop(worker);
        lent.give_up();
        shared.wake.notify_one();
        ran(response)
    }
}

/// A forking worker out of the pool's queue to serve an operation. Dropping it without a word, as a cancelled
/// operation does, tells the pool the worker is gone.
struct Lent<'a> {
    shared: &'a Shared,
    counted: bool,
}

impl Lent<'_> {
    /// The worker is gone: the pool stops counting it, and starts another when it is one short.
    fn give_up(&mut self) {
        if std::mem::take(&mut self.counted) {
            self.shared.state().lent -= 1;
            self.shared.wake.notify_one();
        }
    }
}

impl Drop for Lent<'_> {
    fn drop(&mut self) {
        self.give_up();
    }
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let shared = &self.shared;
        f.debug_struct("Pool")
            .field("command", &shared.command)
            .field("options", &shared.options)
            .field("observer", &shared.observer)
            .field("stats", &self.stats())
            .finish()
    }
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("no panics holding the pool's state")
    }

    fn start(&self) -> Result<Worker, Error> {
        let worker = Worker::start(&self.command);
        let count = if worker.is_ok() { &self.started } else { &self.failed_starts };
        count.fetch_add(1, Ordering::Relaxed);
        worker
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // The task that tops the pool up ends once it finds the pool gone.
        self.wake.notify_one();
    }
}

/// Counts an operation as running for as long as it lives.
struct Running<'a>(&'a Shared);

impl Running<'_> {
    fn enter(shared: &Shared) -> Running<'_> {
        let running = shared.running.fetch_add(1, Ordering::Relaxed) + 1;
        shared.most_running.fetch_max(running, Ordering::Relaxed);
        Running(shared)
    }
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.running.fetch_sub(1, Ordering::Relaxed);
    }
}

fn closed() -> Error {
    Error::Worker { message: "the pool is closed".into() }
}

/// Keeps the pool's waiting workers at their number: starts one whenever an operation has taken one, and tries
/// again, at growing intervals, while workers fail to start. Ends when the pool is closed or dropped.
async fn top_up(pool: Weak<Shared>, wake: Arc<Notify>) {
    let mut retry = FIRST_RETRY;
    loop {
        let Some(shared) = pool.upgrade() else { return };
        let mut failed = false;
        loop {
            {
                let state = shared.state();
                if state.closed {
                    return;
                }
                if state.waiting.len() + state.lent >= shared.options.waiting {
                    break;
                }
            }
            let Ok(worker) = shared.start() else {
                failed = true;
                break;
            };
            let mut state = shared.state();
            if state.closed {
                return;
            }
            state.waiting.push_back(worker);
        }
        drop(shared);
        if failed {
            tokio::time::sleep(retry).await;
            retry = (retry * 2).min(LAST_RETRY);
        } else {
            retry = FIRST_RETRY;
            wake.notified().await;
        }
    }
}
