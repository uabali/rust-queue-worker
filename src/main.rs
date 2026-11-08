//! CLI demo: enqueue N jobs, run a worker pool, print metrics.

use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::sync::watch;
use tracing::info;
use tracing_subscriber::EnvFilter;

use rust_queue_worker::{
    BackoffConfig, Job, Metrics, WorkerPoolConfig, bounded, join_workers, mark_producer_finished,
    new_dead_letter, new_pending, new_producer_done, spawn_pool,
};

#[derive(Parser, Debug)]
#[command(
    name = "rust-queue-worker",
    about = "Tokio in-memory job queue + worker pool with retry/backoff"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a self-contained demo: enqueue jobs, process with workers, print stats.
    Run {
        /// Number of worker tasks
        #[arg(long, default_value_t = 4)]
        workers: usize,

        /// How many jobs to enqueue
        #[arg(long, default_value_t = 20)]
        jobs: u32,

        /// Simulated failure probability per attempt (0.0–1.0)
        #[arg(long, default_value_t = 0.3)]
        fail_rate: f64,

        /// Max attempts per job (including the first)
        #[arg(long, default_value_t = 5)]
        max_attempts: u32,

        /// Bounded queue capacity (backpressure when full)
        #[arg(long, default_value_t = 8)]
        queue_capacity: usize,

        /// Simulated work duration per attempt (ms)
        #[arg(long, default_value_t = 25)]
        work_ms: u64,

        /// Backoff base delay (ms)
        #[arg(long, default_value_t = 100)]
        backoff_base_ms: u64,

        /// Backoff cap (ms)
        #[arg(long, default_value_t = 5000)]
        backoff_cap_ms: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Run {
            workers,
            jobs,
            fail_rate,
            max_attempts,
            queue_capacity,
            work_ms,
            backoff_base_ms,
            backoff_cap_ms,
        } => {
            run_demo(
                workers,
                jobs,
                fail_rate,
                max_attempts,
                queue_capacity,
                work_ms,
                backoff_base_ms,
                backoff_cap_ms,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_demo(
    workers: usize,
    jobs: u32,
    fail_rate: f64,
    max_attempts: u32,
    queue_capacity: usize,
    work_ms: u64,
    backoff_base_ms: u64,
    backoff_cap_ms: u64,
) -> Result<()> {
    let metrics = Metrics::new();
    let dead_letter = new_dead_letter();
    let pending = new_pending();
    let producer_done = new_producer_done();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let (tx, rx) = bounded(queue_capacity);
    let retry_tx = tx.clone();

    let pool_cfg = WorkerPoolConfig {
        workers,
        fail_rate,
        work_ms,
        backoff: BackoffConfig {
            base: Duration::from_millis(backoff_base_ms),
            cap: Duration::from_millis(backoff_cap_ms),
        },
    };

    info!(
        workers,
        jobs, fail_rate, max_attempts, queue_capacity, "starting demo"
    );

    let handles = spawn_pool(
        rx,
        retry_tx,
        metrics.clone(),
        dead_letter.clone(),
        pending.clone(),
        producer_done.clone(),
        shutdown_tx.clone(),
        shutdown_rx,
        pool_cfg,
    );

    let mut enqueue_error = None;
    for i in 0..jobs {
        let payload = format!("job-{i}");
        pending.fetch_add(1, Ordering::SeqCst);
        if let Err(err) = tx.enqueue(Job::new(payload, max_attempts)).await {
            // The job never entered the queue; don't leave pending stuck above 0.
            pending.fetch_sub(1, Ordering::SeqCst);
            enqueue_error = Some(err);
            break;
        }
        metrics.enqueued.fetch_add(1, Ordering::Relaxed);
    }

    // No more new work from the producer. Workers keep `retry_tx` for retries.
    // Shutdown waits for this flag AND pending==0, so a fast worker cannot
    // close the channel between submissions.
    drop(tx);
    mark_producer_finished(&pending, &producer_done, &shutdown_tx);
    join_workers(handles).await;
    if let Some(err) = enqueue_error {
        return Err(err.into());
    }

    let snap = metrics.snapshot();
    let dl = dead_letter.lock().await;
    info!(%snap, dead_letter_count = dl.len(), "run complete");
    println!("=== summary ===");
    println!("{snap}");
    if !dl.is_empty() {
        println!("dead-lettered jobs:");
        for j in dl.iter() {
            println!("  {} payload={} attempts={}", j.id, j.payload, j.attempt);
        }
    }

    Ok(())
}
