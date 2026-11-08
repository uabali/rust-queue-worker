//! In-memory job queue + Tokio worker pool with exponential backoff retries.
//!
//! Lab project for distributed-systems fundamentals: backpressure, retries,
//! worker pools, and dead-letter handling — without a real broker.

pub mod backoff;
pub mod job;
pub mod metrics;
pub mod queue;
pub mod worker;

pub use backoff::{BackoffConfig, delay_after_failure, delay_without_jitter};
pub use job::Job;
pub use metrics::{Metrics, MetricsSnapshot};
pub use queue::{EnqueueError, JobReceiver, JobSender, TryEnqueueError, bounded};
pub use worker::{
    DeadLetter, Pending, ProcessResult, ProducerDone, WorkerPoolConfig, join_workers,
    mark_producer_finished, new_dead_letter, new_pending, new_producer_done, process_job,
    spawn_pool,
};
