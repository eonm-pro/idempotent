use sha2::{Digest, Sha256};
use std::{
    io::Write,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id:         String,
    pub command:    String,
    pub input:      String,
    pub input_var: Option<String>,
    pub start_time: Option<u128>,
    pub end_time:   Option<u128>,
    pub status:     JobStatus,
    pub stdout:     Option<String>,
    pub stderr:     Option<String>,
    pub exit_code:  Option<i32>,
    pub error:      Option<String>,
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
        h.update(b"\0"); // separator so ("ab","c") ≠ ("a","bc")
        h.update(input.as_bytes());
        let id = format!("{:x}", h.finalize());

        Self {
            id,
            command,
            input,
            input_var,
            start_time: None,
            end_time:   None,
            status:     JobStatus::Pending,
            stdout:     None,
            stderr:     None,
            exit_code:  None,
            error:      None,
        }
    }

    /// Spawn `sh -c <command>`, pipe `self.input` to stdin, collect output.
    /// Never panics — OS/IO errors are captured in the job's `error` field.
    pub fn run(mut self) -> Self {
        self.start_time = Some(now_ms());
        self.status     = JobStatus::Running;

        let mut cmd = Command::new("sh");
        cmd.arg("-c")
        .arg(&self.command)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

        if let Some(ref input_var) = self.input_var {
            cmd.stdin(Stdio::null());
            cmd.env(input_var, &self.input);
        } else {
            cmd.stdin(Stdio::piped());
        }

        let mut child = match cmd.spawn() {
            Ok(c)  => c,
            Err(e) => return self.into_error(e.to_string()),
        };

        // Only write stdin when no input_var is set.
        if self.input_var.is_none() {
            if let Some(mut stdin) = child.stdin.take() {
                if let Err(e) = stdin.write_all(self.input.as_bytes()) {
                    if e.kind() != std::io::ErrorKind::BrokenPipe {
                        return self.into_error(e.to_string());
                    }
                }
            }
        }

        let output = match child.wait_with_output() {
            Ok(o)  => o,
            Err(e) => return self.into_error(e.to_string()),
        };

        let stdout    = utf8_or_lossy(output.stdout);
        let stderr    = utf8_or_lossy(output.stderr);
        let exit_code = output.status.code().unwrap_or(-1);

        self.end_time  = Some(now_ms());
        self.exit_code = Some(exit_code);
        self.stderr    = Some(stderr.clone());
        self.stdout    = Some(stdout);

        if exit_code == 0 {
            self.status = JobStatus::Done;
        } else {
            self.status = JobStatus::Errored;
            self.error  = Some(stderr);
        }

        self
    }

    fn into_error(mut self, msg: String) -> Self {
        self.status   = JobStatus::Errored;
        self.error    = Some(msg);
        self.end_time = Some(now_ms());
        self
    }
}

#[inline]
fn utf8_or_lossy(bytes: Vec<u8>) -> String {
    match String::from_utf8(bytes) {
        Ok(s)  => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    }
}

#[inline]
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
