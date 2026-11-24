# CLAUDE.md

In-memory job queue with a Tokio worker pool, exponential backoff + jitter retries and a dead-letter list. Library crate (`src/lib.rs`) plus a CLI demo (`src/main.rs`). Rust edition 2024 (needs Rust 1.85+). No external I/O, no broker.

## Commands

```bash
cargo build
cargo test                      # unit tests live in `#[cfg(test)]` modules under src/
cargo run -- run                # demo with defaults
cargo run -- run --workers 8 --jobs 50 --fail-rate 0.4 --max-attempts 5 --queue-capacity 16
RUST_LOG=warn cargo run -- run  # log level via EnvFilter (default: info)
cargo fmt --check && cargo clippy --all-targets
```

## Layout

- `src/job.rs`: `Job` model (id, payload, attempt, max_attempts, created_at)
- `src/queue.rs`: bounded `tokio::sync::mpsc` wrapper (`enqueue` awaits, `try_enqueue` returns `Full`)
- `src/backoff.rs`: `base * 2^(n-1)` capped, with full jitter
- `src/worker.rs`: worker pool, simulated failures, dead-letter list, pending counter, shutdown
- `src/metrics.rs`: atomic counters (enqueued / processed / succeeded / failed / retries / dead_lettered)
- `src/main.rs`: clap CLI, single `run` subcommand

## Gotchas

- Shutdown: workers exit only when the producer has called `mark_producer_finished` AND `pending == 0`. Checking `pending == 0` alone loses jobs enqueued later.
- Retries sleep on detached tasks and then re-enqueue; sleeping inline on the bounded channel can deadlock when it is full.
- `processed` counts attempts, `retries` counts re-enqueues, so `processed` can exceed `enqueued`.
- `Cargo.lock` is committed on purpose (binary application).
- Do not change public re-exports in `lib.rs` or retry/shutdown semantics without updating the tests in `worker.rs`.
