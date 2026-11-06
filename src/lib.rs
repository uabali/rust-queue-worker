//! In-memory job queue + Tokio worker pool with exponential backoff retries.
//!
//! Lab project for distributed-systems fundamentals: backpressure, retries,
//! worker pools, and dead-letter handling — without a real broker.

pub mod backoff;
pub mod job;
pub mod metrics;
pub mod queue;
pub mod worker;

pub use backoff::{delay_after_failure, delay_without_jitter, BackoffConfig};
pub use job::Job;
pub use metrics::{Metrics, MetricsSnapshot};
pub use queue::{bounded, EnqueueError, JobReceiver, JobSender, TryEnqueueError};
pub use worker::{
    join_workers, new_dead_letter, new_pending, process_job, spawn_pool, DeadLetter, Pending,
    ProcessResult, WorkerPoolConfig,
};
