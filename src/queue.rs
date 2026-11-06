//! Bounded in-memory job queue backed by `tokio::sync::mpsc`.
//!
//! A full channel applies backpressure: `enqueue` awaits until capacity frees.

use tokio::sync::mpsc;

use crate::job::Job;

/// Handle for producers. Cloning shares the same underlying sender.
#[derive(Clone)]
pub struct JobSender {
    tx: mpsc::Sender<Job>,
}

impl JobSender {
    /// Enqueue a job. Waits if the queue is at capacity (backpressure).
    pub async fn enqueue(&self, job: Job) -> Result<(), EnqueueError> {
        self.tx
            .send(job)
            .await
            .map_err(|_| EnqueueError::Closed)
    }

    /// Try to enqueue without waiting. Returns `Full` when at capacity.
    pub fn try_enqueue(&self, job: Job) -> Result<(), TryEnqueueError> {
        match self.tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(TryEnqueueError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(TryEnqueueError::Closed),
        }
    }
}

/// Consumer half used by workers.
pub struct JobReceiver {
    rx: mpsc::Receiver<Job>,
}

impl JobReceiver {
    pub async fn recv(&mut self) -> Option<Job> {
        self.rx.recv().await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueueError {
    #[error("queue closed")]
    Closed,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TryEnqueueError {
    #[error("queue full (backpressure)")]
    Full,
    #[error("queue closed")]
    Closed,
}

/// Create a bounded job queue. `capacity` must be >= 1.
pub fn bounded(capacity: usize) -> (JobSender, JobReceiver) {
    let capacity = capacity.max(1);
    let (tx, rx) = mpsc::channel(capacity);
    (JobSender { tx }, JobReceiver { rx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;

    #[tokio::test]
    async fn enqueue_and_recv() {
        let (tx, mut rx) = bounded(2);
        tx.enqueue(Job::new("a", 3)).await.unwrap();
        let job = rx.recv().await.unwrap();
        assert_eq!(job.payload, "a");
    }

    #[tokio::test]
    async fn try_enqueue_reports_full() {
        let (tx, _rx) = bounded(1);
        tx.try_enqueue(Job::new("1", 1)).unwrap();
        let err = tx.try_enqueue(Job::new("2", 1)).unwrap_err();
        assert_eq!(err, TryEnqueueError::Full);
    }
}
