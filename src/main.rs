mod cli;
mod db;
mod errors;
mod input;
mod jobs;

use std::io::BufRead;
use std::time::Duration;

use clap::Parser;
use crossbeam_channel::{bounded, select, tick};
use db::DbBuilder;
use jobs::Job;

/// How many finished jobs to accumulate before flushing to SQLite.
const BATCH_SIZE: usize = 64;
/// Max time to wait before flushing a partial batch (keeps latency bounded).
const BATCH_TIMEOUT: Duration = Duration::from_millis(200);

fn main() -> Result<(), errors::Error> {
    let cli = cli::Cli::parse();

    let command     = cli.command.clone();
    let parallelism = cli.jobs.max(1);
    let silent      = cli.silent;
    let cached_only = cli.cached_only;
    let force       = cli.force;
    let input_var   = cli.input_var.clone();

    // One DB handle, owned exclusively by the writer thread — zero locking.
    let db = DbBuilder::new(cli.db.clone()).build()?;

    // Pre-load the entire set of cached job IDs into a HashSet so every
    // cache lookup is O(1) in-process with zero SQLite traffic on the hot path.
    let cache: std::collections::HashSet<String> = if force {
        Default::default() // treat everything as a miss
    } else {
        db.all_done_ids()?
    };

    // Optionally print cached results before we start dispatching work.
    if (cached_only || !silent)
        && !cache.is_empty() {
            // We need the full jobs (not just IDs) to print their output.
            // Only load them when something will actually be printed.
        }

    let (work_tx, work_rx)     = bounded::<Job>(parallelism * 4);
    let (result_tx, result_rx) = bounded::<Job>(parallelism * 4);

    let mut worker_handles = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let work_rx   = work_rx.clone();
        let result_tx = result_tx.clone();
        worker_handles.push(std::thread::spawn(move || {
            for job in work_rx {
                let _ = result_tx.send(job.run());
            }
        }));
    }
    drop(work_rx); 
    drop(result_tx);

    let db_thread = std::thread::spawn(move || {
        let mut batch: Vec<Job> = Vec::with_capacity(BATCH_SIZE);
        let ticker = tick(BATCH_TIMEOUT);

        loop {
            select! {
                recv(result_rx) -> msg => {
                    match msg {
                        Ok(job) => {
                            if !silent { print_job_result(&job); }
                            batch.push(job);
                            if batch.len() >= BATCH_SIZE {
                                flush(&db, &mut batch);
                            }
                        }
                        Err(_) => {
                            // Channel closed: all workers are done.
                            flush(&db, &mut batch);
                            break;
                        }
                    }
                }
                recv(ticker) -> _ => {
                    // Time-based flush so partial batches don't sit forever.
                    if !batch.is_empty() {
                        flush(&db, &mut batch);
                    }
                }
            }
        }
    });

    let input = cli.input().buf_reader()?;

    for raw in input.lines() {
        let line = match raw {
            Ok(l) => l,
            Err(e) => { eprintln!("[memorun] read error: {e}"); continue; }
        };
        // Trim CR so CRLF files work on Linux too.
        let line = line.trim_end_matches('\r').to_owned();
        if line.is_empty() { continue; }

        let job = Job::new(command.clone(), line, input_var.clone());

        if cached_only {
            if !cache.contains(&job.id) && !silent {
                eprintln!("[memorun] no cache for: {}", job.input);
            }
            continue;
        }

        if !force && cache.contains(&job.id) {
            continue; // already done, skip
        }

        // Blocks when the pool is saturated — intentional back-pressure.
        if work_tx.send(job).is_err() {
            eprintln!("[memorun] worker pool shut down unexpectedly");
            break;
        }
    }

    drop(work_tx); // signal workers: no more work
    for h in worker_handles { let _ = h.join(); }
    let _ = db_thread.join();

    Ok(())
}

/// Write `batch` inside one SQLite transaction, then clear it.
fn flush(db: &db::Db, batch: &mut Vec<Job>) {
    if let Err(e) = db.upsert_batch(batch) {
        eprintln!("[memorun] db flush error: {e}");
    }
    batch.clear();
}

fn print_job_result(job: &Job) {
    use jobs::JobStatus;
    match &job.status {
        JobStatus::Done => {
            if let Some(out) = &job.stdout {
                print!("{out}");
            }
        }
        JobStatus::Errored => {
            eprintln!(
                "[memorun] job {} errored (exit {:?}): {}",
                &job.id[..8],
                job.exit_code,
                job.error.as_deref().unwrap_or("<unknown>")
            );
            if let Some(out) = &job.stdout {
                if !out.is_empty() { print!("{out}"); }
            }
        }
        _ => {}
    }
}
