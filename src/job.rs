//! Job model for the in-memory queue.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A unit of work submitted to the queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub payload: String,
    pub attempt: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
}

impl Job {
    pub fn new(payload: impl Into<String>, max_attempts: u32) -> Self {
        Self {
            id: Uuid::new_v4(),
            payload: payload.into(),
            attempt: 0,
            max_attempts,
            created_at: Utc::now(),
        }
    }

    /// Bump attempt counter before (re)processing.
    pub fn bump_attempt(&mut self) {
        self.attempt = self.attempt.saturating_add(1);
    }

    pub fn attempts_exhausted(&self) -> bool {
        self.attempt >= self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_job_starts_at_zero_attempts() {
        let j = Job::new("hello", 3);
        assert_eq!(j.attempt, 0);
        assert_eq!(j.max_attempts, 3);
        assert!(!j.attempts_exhausted());
    }

    #[test]
    fn bump_and_exhaust() {
        let mut j = Job::new("x", 2);
        j.bump_attempt();
        assert_eq!(j.attempt, 1);
        assert!(!j.attempts_exhausted());
        j.bump_attempt();
        assert!(j.attempts_exhausted());
    }
}
