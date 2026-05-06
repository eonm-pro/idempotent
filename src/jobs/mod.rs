use sha2::{Digest, Sha256};
use std::{
    io::Write,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: String,
    pub command: String,
    pub input: String,
    pub start_time: Option<u128>,
    pub end_time: Option<u128>,
    pub status: JobStatus,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    Errored,
}

impl Job {
    pub fn new(command: String, input: String) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(command.as_bytes());
        hasher.update(input.as_bytes());
        let hash = hasher.finalize();
        let id = format!("{:x}", hash);
        Self {
            id,
            command,
            input,
            start_time: None,
            end_time: None,
            status: JobStatus::Pending,
            stdout: None,
            stderr: None,
            exit_code: None,
            error: None,
        }
    }

    pub fn run(mut self) -> crate::jobs::Job {
        self.start_time = Some(now_ms());
        self.status = crate::jobs::JobStatus::Running;
        self.error = None;

        let mut child = match Command::new("sh")
            .arg("-c")
            .arg(self.command.clone())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                self.status = crate::jobs::JobStatus::Errored;
                self.error = Some(e.to_string());
                self.end_time = Some(now_ms());
                return self;
            }
        };

        // Write input to the child's stdin, then close it so the process can proceed.
        if let Some(mut stdin) = child.stdin.take() {
            if let Err(e) = stdin.write_all(self.input.as_bytes()) {
                self.status = crate::jobs::JobStatus::Errored;
                self.error = Some(e.to_string());
                self.end_time = Some(now_ms());
                return self;
            }
        }

        let output = match child.wait_with_output() {
            Ok(output) => output,
            Err(e) => {
                self.status = crate::jobs::JobStatus::Errored;
                self.error = Some(e.to_string());
                self.end_time = Some(now_ms());
                return self;
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1); // i32

        self.end_time = Some(now_ms());
        self.exit_code = Some(exit_code);
        self.stderr = Some(stderr.clone());

        if exit_code == 0 {
            self.status = crate::jobs::JobStatus::Done;
            self.stdout = Some(stdout);
        } else {
            self.status = crate::jobs::JobStatus::Errored;
            self.stdout = Some(stdout);
            self.error = Some(stderr);
        }

        self
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
