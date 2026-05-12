use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender};
use log::{debug, error};

use crate::errors::Error;
use crate::jobs::{Job, JobStatus};
use crate::shell::PersistentShell;

/// Spawn `workers` threads. Each pulls a `Job` from `work_rx`, executes it
/// in a persistent shell, and sends the completed `Job` to `result_tx`.
/// Threads exit naturally when `work_rx` is closed.
pub fn spawn(workers: usize, work_rx: Receiver<Job>, result_tx: Sender<Result<Job, Error>>) {
    for i in 0..workers {
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();

        std::thread::spawn(move || {
            debug!("worker {i} started");

            let mut shell = match PersistentShell::new() {
                Ok(s) => s,
                Err(e) => {
                    error!("worker {i} failed to start shell: {e}");
                    let _ = result_tx.send(Err(Error::Shell(e.to_string())));
                    return;
                }
            };

            for job in &work_rx {
                debug!("worker {i} running job {}", &job.id[..8]);
                let _ = result_tx.send(Ok(execute(job, &mut shell)));
            }

            debug!("worker {i} exiting");
        });
    }
}

/// Run a job in the given shell, returning the completed job.
fn execute(mut job: Job, shell: &mut PersistentShell) -> Job {
    job.start_time = Some(now_ms());
    job.status = JobStatus::Running;

    let (cmd, stdin_input) = build_command(&job);

    match shell.run(&cmd, stdin_input.as_deref()) {
        Ok(result) => {
            job.end_time = Some(now_ms());
            job.exit_code = Some(result.exit_code);
            job.stdout = Some(result.stdout);
            job.stderr = Some(result.stderr.clone());

            if result.exit_code == 0 {
                job.status = JobStatus::Done;
            } else {
                job.status = JobStatus::Errored;
                job.error = Some(result.stderr);
            }
        }
        Err(e) => {
            job.end_time = Some(now_ms());
            job.status = JobStatus::Errored;
            job.error = Some(e.to_string());
        }
    }

    job
}

/// Returns `(command_string, optional_stdin_input)`.
/// When `input_var` is set the input is injected as an env var and stdin is
/// left as /dev/null; otherwise the raw input is piped via stdin.
fn build_command(job: &Job) -> (String, Option<String>) {
    match &job.input_var {
        Some(var) => {
            let cmd = format!(
                "export {}={}; {}",
                var,
                shell_quote(&job.input),
                job.command,
            );
            (cmd, None)
        }
        None => (job.command.clone(), Some(job.input.clone())),
    }
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
