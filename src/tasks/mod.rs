use std::{
    io::Write,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Task;

impl Task {
    /// Run `shell_command` with `job.input` piped to stdin.
    /// The job's stdout/stderr/exit_code are populated on return.
    pub fn run(&self, mut job: crate::jobs::Job, shell_command: &str) -> crate::jobs::Job {
        job.start_time = Some(now_ms());
        job.status = crate::jobs::JobStatus::Running;
        job.error = None;

        let mut child = match Command::new("sh")
            .arg("-c")
            .arg(shell_command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                job.status = crate::jobs::JobStatus::Errored;
                job.error = Some(e.to_string());
                job.end_time = Some(now_ms());
                return job;
            }
        };

        // Write input to the child's stdin, then close it so the process can proceed.
        if let Some(mut stdin) = child.stdin.take() {
            if let Err(e) = stdin.write_all(job.input.as_bytes()) {
                job.status = crate::jobs::JobStatus::Errored;
                job.error = Some(e.to_string());
                job.end_time = Some(now_ms());
                return job;
            }
        }

        let output = match child.wait_with_output() {
            Ok(output) => output,
            Err(e) => {
                job.status = crate::jobs::JobStatus::Errored;
                job.error = Some(e.to_string());
                job.end_time = Some(now_ms());
                return job;
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1); // i32

        job.end_time = Some(now_ms());
        job.exit_code = Some(exit_code);
        job.stderr = Some(stderr.clone());

        if exit_code == 0 {
            job.status = crate::jobs::JobStatus::Done;
            job.stdout = Some(stdout);
        } else {
            job.status = crate::jobs::JobStatus::Errored;
            job.stdout = Some(stdout);
            job.error = Some(stderr);
        }

        job
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
