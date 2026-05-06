use sha2::{Sha256, Digest};

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
}