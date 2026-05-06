use std::{
    io::Write,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Task;

impl Task {
    pub fn run(&self, mut job: crate::jobs::Job, shell_command: String) -> crate::jobs::Job {
        job.start_time = Some(now_ms());
        job.status = crate::jobs::JobStatus::Runing;
        job.error = None;

        let mut child = match Command::new("sh")
            .arg("-c")
            .arg(&shell_command)
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

        let end_time = now_ms();

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1);

        job.end_time = Some(end_time);
        job.exit_code = exit_code as usize;

        if exit_code == 0 {
            job.status = crate::jobs::JobStatus::Done;
            job.output = Some(stdout);
        } else {
            job.status = crate::jobs::JobStatus::Errored;
            job.error = Some(stderr);
            job.output = Some(stdout);
        }

        job
    }
}

fn now_ms() -> usize {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as usize
}