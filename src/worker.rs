//! Worker pool: pull jobs, simulate work with a configurable fail rate, retry.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::backoff::{delay_after_failure, BackoffConfig};
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

/// Counts jobs not yet succeeded or dead-lettered. Hits 0 → shutdown.
pub type Pending = Arc<AtomicUsize>;

pub fn new_pending() -> Pending {
    Arc::new(AtomicUsize::new(0))
}

fn mark_done(pending: &Pending, shutdown_tx: &watch::Sender<bool>) {
    let prev = pending.fetch_sub(1, Ordering::SeqCst);
    if prev == 1 {
        let _ = shutdown_tx.send(true);
    }
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
            mark_done(&pending, &shutdown_tx);
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
                        mark_done(&pending, &shutdown_tx);
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
                            mark_done(&pending, &shutdown_tx);
                        } else {
                            schedule_retry(
                                job,
                                retry_tx.clone(),
                                Arc::clone(&metrics),
                                Arc::clone(&dead_letter),
                                Arc::clone(&pending),
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
