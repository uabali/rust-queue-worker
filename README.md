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
**Shutdown:** `pending` counts unfinished jobs. Workers exit only after the producer calls `mark_producer_finished` AND `pending` is 0. Exiting on `pending == 0` alone drops later jobs: a fast worker can finish the first job and close the channel before the next `enqueue`.

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
- Queue is not durable — restart loses in-flight jobs. There is no broker, ack, or reclaim.
- Retries run on detached tasks (sleep, then re-enqueue). Sleeping inline on the same bounded channel can deadlock when the queue is full.
- `cargo test` covers queue bounds, backoff math, the early-shutdown loss above, and exhausted retries on a capacity-1 queue. It does not measure latency and it is not a production worker.

## License

MIT
