# rust-queue-worker

In-memory job queue with a Tokio worker pool, exponential backoff + jitter retries, and a dead-letter list.

## Architecture

```
producer (CLI) ──enqueue──► bounded mpsc queue ◄──retry── workers
                                │
                                ▼
                          N worker tasks
                                │
                    success ──► metrics.succeeded
                    fail + attempts left ──► sleep(backoff) ──► re-enqueue
                    fail + exhausted ──► dead-letter
```

| Module | Role |
|--------|------|
| `job` | Job model (`id`, `payload`, `attempt`, `max_attempts`, `created_at`) |
| `queue` | Bounded `tokio::sync::mpsc` (`enqueue` waits; `try_enqueue` returns `Full`) |
| `backoff` | Exponential backoff with full jitter (`base * 2^(n-1)`, capped) |
| `worker` | Worker pool, simulated fail rate, dead-letter, pending/shutdown |
| `metrics` | Counters: enqueued / processed / succeeded / failed / retries / dead_lettered |

**Backpressure:** queue capacity is bounded.  
**Shutdown:** a `pending` counter tracks unfinished jobs; when it hits zero, workers exit via a `watch` channel.

## Requirements

- Rust 1.85+
- Cargo

## Run

```bash
cargo run -- run
cargo run -- run --workers 8 --jobs 50 --fail-rate 0.4 --max-attempts 5 --queue-capacity 16
RUST_LOG=warn cargo run -- run
```

Example summary:

```text
=== summary ===
enqueued=20 processed=28 succeeded=18 failed=10 retries=8 dead_lettered=2
```

`processed` counts attempts; `retries` counts re-enqueues.

## Test

```bash
cargo test
```

## Notes

- Failures are simulated via `--fail-rate` (no external I/O).
- Queue is not durable — restart loses in-flight jobs.
- Retries run on detached tasks (sleep, then re-enqueue). Sleeping inline on the same bounded channel can deadlock when the queue is full.

## License

MIT
