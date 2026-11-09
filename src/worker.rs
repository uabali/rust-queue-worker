//! Worker pool: pull jobs, simulate work with a configurable fail rate, retry.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use rand::RngExt;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::backoff::{BackoffConfig, delay_after_failure};
use crate::job::Job;
use crate::metrics::Metrics;
use crate::queue::{JobReceiver, JobSender};

/// Outcome of processing a single attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessResult {
    Success,
    TransientFailure,
}

/// Simulated processor: sleeps briefly, then fails with probability `fail_rate`.
pub async fn process_job(_job: &Job, fail_rate: f64, work_ms: u64) -> ProcessResult {
    if work_ms > 0 {
        tokio::time::sleep(Duration::from_millis(work_ms)).await;
    }
    let fail_rate = fail_rate.clamp(0.0, 1.0);
    if rand::rng().random_bool(fail_rate) {
        ProcessResult::TransientFailure
    } else {
        ProcessResult::Success
    }
}

pub struct WorkerPoolConfig {
    pub workers: usize,
    pub fail_rate: f64,
    pub work_ms: u64,
    pub backoff: BackoffConfig,
}

/// Shared dead-letter store for exhausted jobs.
pub type DeadLetter = Arc<Mutex<Vec<Job>>>;

pub fn new_dead_letter() -> DeadLetter {
    Arc::new(Mutex::new(Vec::new()))
}

/// Counts jobs not yet succeeded or dead-lettered.
pub type Pending = Arc<AtomicUsize>;

pub fn new_pending() -> Pending {
    Arc::new(AtomicUsize::new(0))
}

/// Set by the producer after it has finished submitting (or given up).
/// Workers shut down only when this is true AND `pending` is 0.
/// Shutting down on `pending == 0` alone drops later submissions: a fast
/// worker can finish job 1 and close the channel before job 2 is enqueued.
pub type ProducerDone = Arc<AtomicBool>;

pub fn new_producer_done() -> ProducerDone {
    Arc::new(AtomicBool::new(false))
}

fn maybe_shutdown(
    pending: &Pending,
    producer_done: &ProducerDone,
    shutdown_tx: &watch::Sender<bool>,
) {
    if producer_done.load(Ordering::SeqCst) && pending.load(Ordering::SeqCst) == 0 {
        let _ = shutdown_tx.send(true);
    }
}

fn mark_done(pending: &Pending, producer_done: &ProducerDone, shutdown_tx: &watch::Sender<bool>) {
    let prev = pending.fetch_sub(1, Ordering::SeqCst);
    if prev == 0 {
        warn!("pending counter underflow; ignoring");
        return;
    }
    maybe_shutdown(pending, producer_done, shutdown_tx);
}

/// Call once the producer will not enqueue any more new jobs.
pub fn mark_producer_finished(
    pending: &Pending,
    producer_done: &ProducerDone,
    shutdown_tx: &watch::Sender<bool>,
) {
    producer_done.store(true, Ordering::SeqCst);
    maybe_shutdown(pending, producer_done, shutdown_tx);
}

/// Schedule a retry on a detached task so workers keep draining the queue.
///
/// Important: workers must NOT sleep+re-enqueue inline on the same bounded
/// channel — that deadlocks when every worker is blocked on a full queue
/// and nobody is receiving.
fn schedule_retry(
    job: Job,
    retry_tx: JobSender,
    metrics: Arc<Metrics>,
    dead_letter: DeadLetter,
    pending: Pending,
    producer_done: ProducerDone,
    shutdown_tx: watch::Sender<bool>,
    backoff: BackoffConfig,
) {
    let sleep_for = delay_after_failure(&backoff, job.attempt);
    warn!(
        job_id = %job.id,
        attempt = job.attempt,
        ?sleep_for,
        "transient failure; scheduling retry"
    );
    metrics
        .retries
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    tokio::spawn(async move {
        tokio::time::sleep(sleep_for).await;
        if let Err(e) = retry_tx.enqueue(job.clone()).await {
            warn!(error = %e, job_id = %job.id, "retry enqueue failed; dead-lettering");
            metrics
                .dead_lettered
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            dead_letter.lock().await.push(job);
            mark_done(&pending, &producer_done, &shutdown_tx);
        }
    });
}

/// Spawn `workers` tasks sharing one receiver (via Mutex).
pub fn spawn_pool(
    rx: JobReceiver,
    retry_tx: JobSender,
    metrics: Arc<Metrics>,
    dead_letter: DeadLetter,
    pending: Pending,
    producer_done: ProducerDone,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    cfg: WorkerPoolConfig,
) -> Vec<JoinHandle<()>> {
    let workers = cfg.workers.max(1);
    let shared_rx = Arc::new(Mutex::new(rx));
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let shared_rx = Arc::clone(&shared_rx);
        let retry_tx = retry_tx.clone();
        let metrics = Arc::clone(&metrics);
        let dead_letter = Arc::clone(&dead_letter);
        let pending = Arc::clone(&pending);
        let producer_done = Arc::clone(&producer_done);
        let shutdown_tx = shutdown_tx.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        let fail_rate = cfg.fail_rate;
        let work_ms = cfg.work_ms;
        let backoff = cfg.backoff;

        handles.push(tokio::spawn(async move {
            loop {
                if *shutdown_rx.borrow() {
                    info!(worker_id, "shutdown signaled; worker exiting");
                    break;
                }

                let job_opt = {
                    let mut guard = shared_rx.lock().await;
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => None,
                        job = guard.recv() => job,
                    }
                };

                let Some(mut job) = job_opt else {
                    if *shutdown_rx.borrow() {
                        info!(worker_id, "shutdown; worker exiting");
                    } else {
                        info!(worker_id, "queue closed; worker exiting");
                    }
                    break;
                };

                job.bump_attempt();
                metrics
                    .processed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                info!(
                    worker_id,
                    job_id = %job.id,
                    attempt = job.attempt,
                    max_attempts = job.max_attempts,
                    payload = %job.payload,
                    "processing"
                );

                match process_job(&job, fail_rate, work_ms).await {
                    ProcessResult::Success => {
                        metrics
                            .succeeded
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        info!(worker_id, job_id = %job.id, "succeeded");
                        mark_done(&pending, &producer_done, &shutdown_tx);
                    }
                    ProcessResult::TransientFailure => {
                        metrics
                            .failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        if job.attempts_exhausted() {
                            warn!(
                                worker_id,
                                job_id = %job.id,
                                attempts = job.attempt,
                                "max attempts reached → dead-letter"
                            );
                            metrics
                                .dead_lettered
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            dead_letter.lock().await.push(job);
                            mark_done(&pending, &producer_done, &shutdown_tx);
                        } else {
                            schedule_retry(
                                job,
                                retry_tx.clone(),
                                Arc::clone(&metrics),
                                Arc::clone(&dead_letter),
                                Arc::clone(&pending),
                                Arc::clone(&producer_done),
                                shutdown_tx.clone(),
                                backoff,
                            );
                        }
                    }
                }
            }
        }));
    }

    handles
}

pub async fn join_workers(handles: Vec<JoinHandle<()>>) {
    for h in handles {
        let _ = h.await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::sync::watch;

    use crate::backoff::BackoffConfig;
    use crate::job::Job;
    use crate::metrics::Metrics;
    use crate::queue::{EnqueueError, bounded};

    use super::*;

    fn cfg(workers: usize, fail_rate: f64) -> WorkerPoolConfig {
        WorkerPoolConfig {
            workers,
            fail_rate,
            work_ms: 0,
            backoff: BackoffConfig {
                base: Duration::from_millis(1),
                cap: Duration::from_millis(5),
            },
        }
    }

    struct Harness {
        tx: crate::queue::JobSender,
        metrics: Arc<Metrics>,
        dead_letter: DeadLetter,
        pending: Pending,
        producer_done: ProducerDone,
        shutdown_tx: watch::Sender<bool>,
        handles: Vec<JoinHandle<()>>,
    }

    fn start(capacity: usize, pool: WorkerPoolConfig) -> Harness {
        let metrics = Metrics::new();
        let dead_letter = new_dead_letter();
        let pending = new_pending();
        let producer_done = new_producer_done();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (tx, rx) = bounded(capacity);
        let handles = spawn_pool(
            rx,
            tx.clone(),
            metrics.clone(),
            dead_letter.clone(),
            pending.clone(),
            producer_done.clone(),
            shutdown_tx.clone(),
            shutdown_rx,
            pool,
        );
        Harness {
            tx,
            metrics,
            dead_letter,
            pending,
            producer_done,
            shutdown_tx,
            handles,
        }
    }

    async fn finish(h: Harness) {
        drop(h.tx);
        mark_producer_finished(&h.pending, &h.producer_done, &h.shutdown_tx);
        let joined = tokio::time::timeout(Duration::from_secs(3), join_workers(h.handles)).await;
        assert!(joined.is_ok(), "workers did not shut down");
    }

    #[tokio::test]
    async fn second_job_is_not_dropped_after_first_finishes() {
        let h = start(8, cfg(2, 0.0));

        h.pending.fetch_add(1, Ordering::SeqCst);
        h.tx.enqueue(Job::new("first", 1)).await.unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while h.metrics.snapshot().succeeded < 1 {
            if tokio::time::Instant::now() > deadline {
                panic!("first job did not finish");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;

        h.pending.fetch_add(1, Ordering::SeqCst);
        let enqueued =
            tokio::time::timeout(Duration::from_secs(1), h.tx.enqueue(Job::new("second", 1))).await;
        match enqueued {
            Ok(Ok(())) => {}
            Ok(Err(EnqueueError::Closed)) => {
                panic!("queue closed after the first job; later work was dropped")
            }
            Err(_) => panic!("enqueue timed out"),
        }

        let metrics = h.metrics.clone();
        finish(h).await;
        let snap = metrics.snapshot();
        assert_eq!(snap.succeeded, 2, "{snap}");
        assert_eq!(snap.dead_lettered, 0, "{snap}");
    }

    #[tokio::test]
    async fn exhausted_retries_dead_letter_without_hanging_on_full_queue() {
        // capacity 1 used to deadlock when a worker slept and re-enqueued inline.
        let h = start(1, cfg(2, 1.0));
        let jobs = 4u64;
        for i in 0..jobs {
            h.pending.fetch_add(1, Ordering::SeqCst);
            h.tx.enqueue(Job::new(format!("j{i}"), 3)).await.unwrap();
        }
        let metrics = h.metrics.clone();
        let dead_letter = h.dead_letter.clone();
        finish(h).await;
        let snap = metrics.snapshot();
        assert_eq!(snap.succeeded, 0, "{snap}");
        assert_eq!(snap.dead_lettered, jobs, "{snap}");
        assert_eq!(dead_letter.lock().await.len() as u64, jobs);
    }
}
