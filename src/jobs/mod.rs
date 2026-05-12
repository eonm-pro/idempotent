use sha2::{Digest, Sha256};

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

    pub fn is_done(&self) -> bool {
        self.status == JobStatus::Done
    }
}
