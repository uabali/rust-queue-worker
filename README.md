# rust-queue-worker

Small **Rust + Tokio** lab: in-memory job queue, worker pool, and retries with exponential backoff + jitter.

Built as Distributed Systems practice (queue / backpressure / retry / pool) without standing up Redis or Kafka.

## Turkish summary / Kısa özet

Bu repo, bellek içi bir iş kuyruğu ve worker havuzu çalıştırır. İşler başarısız olursa üstel backoff + jitter ile yeniden denenir; deneme hakkı bitince dead-letter listesine düşer. Amaç: backpressure, retry ve worker pool kavramlarını Tokio ile elle görmek.

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
| `queue` | Bounded `tokio::sync::mpsc` wrapper (async enqueue + `try_enqueue` for full) |
| `backoff` | Exponential backoff with full jitter (`base * 2^(n-1)`, capped) |
| `worker` | Worker pool, simulated fail rate, dead-letter, pending/shutdown |
| `metrics` | Atomic counters: enqueued / processed / succeeded / failed / retries / dead_lettered |

**Backpressure:** queue capacity is bounded. `enqueue` waits when full; `try_enqueue` returns `Full`.

**Shutdown:** a `pending` counter tracks jobs not yet succeeded or dead-lettered. When it hits zero, workers shut down via a `watch` channel (retry senders would otherwise keep the mpsc open forever).

## Requirements

- Rust 1.85+ (developed with rustc 1.95)
- Cargo

## Run

```bash
# default demo: 4 workers, 20 jobs, 30% fail rate
cargo run -- run

# custom
cargo run -- run --workers 8 --jobs 50 --fail-rate 0.4 --max-attempts 5 --queue-capacity 16

# quieter / louder logs
RUST_LOG=warn cargo run -- run
RUST_LOG=debug cargo run -- run --jobs 5 --fail-rate 0.5
```

Example summary line:

```text
=== summary ===
enqueued=20 processed=28 succeeded=18 failed=10 retries=8 dead_lettered=2
```

(`processed` counts attempts; `retries` counts re-enqueues.)

## Test

```bash
cargo test
```

Unit tests cover backoff growth/cap, jitter bounds, job attempt helpers, and queue full/backpressure.

## What you learn

1. **Bounded queue + backpressure** — producers slow down when workers cannot keep up.
2. **Worker pool** — multiple async tasks competing on one queue.
3. **Retry with exponential backoff + jitter** — avoid thundering herds after correlated failures.
4. **Dead-letter** — give up after `max_attempts` and keep the failed jobs for inspection.
5. **Graceful completion** — outstanding-work counter so the process exits cleanly even when retry senders keep the channel alive.

## Design notes

- Failures are **simulated** (`--fail-rate`) so the lab stays deterministic-enough without flaky I/O.
- This is intentionally **not** a durable broker: restart loses the queue. Next labs can swap mpsc for Redis/NATS.
- Jitter uses **full jitter** (`random(0, ceiling)`), a common AWS-style recommendation.

- Retries are scheduled on **detached tasks** (sleep, then re-enqueue). Sleeping/re-enqueueing inline on the same bounded channel deadlocks when every worker blocks on a full queue and nobody receives — a useful distributed-systems footgun.

## License

MIT (personal lab).
