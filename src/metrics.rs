//! Shared counters for a run.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct Metrics {
    pub enqueued: AtomicU64,
    pub processed: AtomicU64,
    pub succeeded: AtomicU64,
    pub failed: AtomicU64,
    pub retries: AtomicU64,
    pub dead_lettered: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            processed: self.processed.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub enqueued: u64,
    pub processed: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub retries: u64,
    pub dead_lettered: u64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enqueued={} processed={} succeeded={} failed={} retries={} dead_lettered={}",
            self.enqueued,
            self.processed,
            self.succeeded,
            self.failed,
            self.retries,
            self.dead_lettered
        )
    }
}
