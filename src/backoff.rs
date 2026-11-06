//! Exponential backoff with full jitter.

use std::time::Duration;

use rand::RngExt;

/// Configuration for exponential backoff with jitter.
#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    /// Base delay for attempt 1 (before exponential growth).
    pub base: Duration,
    /// Hard upper bound on sleep duration.
    pub cap: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(100),
            cap: Duration::from_secs(5),
        }
    }
}

/// Compute sleep duration after a failed attempt.
///
/// Uses "full jitter": `random(0, min(cap, base * 2^(attempt-1)))`.
/// `attempt` is 1-based (first failure → attempt == 1).
pub fn delay_after_failure(cfg: &BackoffConfig, attempt: u32) -> Duration {
    let attempt = attempt.max(1);
    let exp = attempt.saturating_sub(1).min(16); // avoid overflow
    let multiplied = cfg
        .base
        .checked_mul(1u32 << exp)
        .unwrap_or(cfg.cap);
    let ceiling = multiplied.min(cfg.cap);

    if ceiling.is_zero() {
        return Duration::ZERO;
    }

    let ceiling_ms = ceiling.as_millis() as u64;
    let jitter_ms = rand::rng().random_range(0..=ceiling_ms);
    Duration::from_millis(jitter_ms)
}

/// Deterministic delay without jitter (for unit tests / metrics).
pub fn delay_without_jitter(cfg: &BackoffConfig, attempt: u32) -> Duration {
    let attempt = attempt.max(1);
    let exp = attempt.saturating_sub(1).min(16);
    let multiplied = cfg
        .base
        .checked_mul(1u32 << exp)
        .unwrap_or(cfg.cap);
    multiplied.min(cfg.cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_exponentially_until_cap() {
        let cfg = BackoffConfig {
            base: Duration::from_millis(100),
            cap: Duration::from_secs(5),
        };
        assert_eq!(delay_without_jitter(&cfg, 1), Duration::from_millis(100));
        assert_eq!(delay_without_jitter(&cfg, 2), Duration::from_millis(200));
        assert_eq!(delay_without_jitter(&cfg, 3), Duration::from_millis(400));
        assert_eq!(delay_without_jitter(&cfg, 7), Duration::from_millis(6400).min(cfg.cap));
        assert_eq!(delay_without_jitter(&cfg, 10), cfg.cap);
    }

    #[test]
    fn jitter_stays_within_bound() {
        let cfg = BackoffConfig::default();
        for attempt in 1..=8 {
            let d = delay_after_failure(&cfg, attempt);
            let max = delay_without_jitter(&cfg, attempt);
            assert!(d <= max, "attempt {attempt}: {d:?} > {max:?}");
        }
    }
}
