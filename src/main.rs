mod cli;
mod db;
mod errors;
mod input;
mod jobs;
mod pipeline;
mod runner;
mod shell;
mod sink;

use std::collections::HashSet;
use std::io::BufRead;

use clap::Parser;
use log::{debug, warn};

use crate::db::DbBuilder;
use crate::jobs::{Job, JobStatus};
use crate::pipeline::Pipeline;

fn main() -> Result<(), errors::Error> {
    env_logger::init();

    let cli = cli::Cli::parse();

    let db    = DbBuilder::new(cli.db.clone()).build()?;
    let cache = db.all_done_ids()?;

    let silent   = cli.silent;
    let pipeline = Pipeline::new(cli.jobs.max(1), db, move |job| print_job(job, silent));

    for raw in cli.input().buf_reader()?.lines() {
        let line = raw?.trim_end_matches('\r').to_owned();
        if line.is_empty() { continue; }

        let job = Job::new(cli.command.clone(), line, cli.input_var.clone());

        if should_run(&job, &cache, cli.force, cli.cached_only) {
            pipeline.submit(job)?;
        }
    }

    pipeline.wait()
}

fn should_run(job: &Job, cache: &HashSet<String>, force: bool, cached_only: bool) -> bool {
    if cached_only {
        debug!("cached-only mode, skipping job {}", &job.id[..8]);
        return false;
    }
    if !force && cache.contains(&job.id) {
        debug!("skipping cached job {}: {}", &job.id[..8], job.input);
        return false;
    }
    true
}

fn print_job(job: &Job, silent: bool) {
    if silent { return; }
    match job.status {
        JobStatus::Done => {
            if let Some(out) = &job.stdout {
                print!("{out}");
            }
        }
        JobStatus::Errored => {
            warn!(
                "job {} errored (exit {:?}): {}",
                &job.id[..8],
                job.exit_code,
                job.error.as_deref().unwrap_or("<unknown>"),
            );
            if let Some(out) = &job.stdout {
                if !out.is_empty() { print!("{out}"); }
            }
        }
        _ => {}
    }
}
