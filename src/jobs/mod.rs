use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::shell::PersistentShell;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: String,
    pub command: String,
    pub input: String,
    pub input_var: Option<String>,
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
    pub fn new(command: String, input: String, input_var: Option<String>) -> Self {
        let mut h = Sha256::new();
        h.update(command.as_bytes());
        h.update(b"\0");
        h.update(input.as_bytes());
        let id = format!("{:x}", h.finalize());

        Self {
            id,
            command,
            input,
            input_var,
            start_time: None,
            end_time: None,
            status: JobStatus::Pending,
            stdout: None,
            stderr: None,
            exit_code: None,
            error: None,
        }
    }

    /// Run this job using the provided persistent shell.
    pub fn run(mut self, shell: &mut PersistentShell) -> Self {
        self.start_time = Some(now_ms());
        self.status = JobStatus::Running;

        let (cmd, stdin_input) = match &self.input_var {
            Some(var) => {
                let cmd = format!(
                    "export {}={}; {}",
                    var,
                    shell_quote(&self.input),
                    self.command
                );
                (cmd, None)
            }
            None => (self.command.clone(), Some(self.input.as_str())),
        };

        match shell.run(&cmd, stdin_input) {
            Ok(result) => {
                self.end_time = Some(now_ms());
                self.exit_code = Some(result.exit_code);
                self.stdout = Some(result.stdout);
                self.stderr = Some(result.stderr.clone());

                if result.exit_code == 0 {
                    self.status = JobStatus::Done;
                } else {
                    self.status = JobStatus::Errored;
                    self.error = Some(result.stderr);
                }
            }
            Err(e) => {
                self.end_time = Some(now_ms());
                self.status = JobStatus::Errored;
                self.error = Some(e.to_string());
            }
        }

        self
    }
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

#[inline]
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
