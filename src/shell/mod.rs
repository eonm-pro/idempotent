/// A persistent `sh` process that executes commands one at a time.
///
/// One `sh` process lives per worker thread. Commands are sent to its stdin
/// and output is captured via temp files (out, err, sig). Input to the user
/// command is delivered via a named pipe (fifo) — no sentinel, no heredoc
/// delimiter, no way for user data to corrupt the control channel.
///
/// # Protocol (per invocation)
///
/// ```sh
/// ( command ) <fifo >out 2>err; echo $? >sig
/// ```
///
/// The Rust side:
///   1. Creates the fifo with `mkfifo`.
///   2. Spawns a writer thread that opens the fifo and writes the input.
///   3. Sends the wrapper line to persistent `sh` stdin.
///   4. Polls `sig` until it contains the exit code.
///   5. Reads out/err, cleans up all temp files.
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    process::{Child, ChildStdin, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static INVOCATION_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct ShellResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

pub struct PersistentShell {
    child: Child,
    stdin: ChildStdin,
    tmp_dir: PathBuf,
    prefix: String,
}

impl PersistentShell {
    pub fn new() -> io::Result<Self> {
        let tmp_dir = std::env::temp_dir().join(format!(
            "memorun-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos()
        ));
        fs::create_dir_all(&tmp_dir)?;

        let prefix = format!("{}", std::process::id());

        let mut child = Command::new("sh")
            .arg("--norc")
            .arg("--noprofile")
            .arg("-u")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let stdin = child.stdin.take().expect("stdin was piped");

        Ok(Self {
            child,
            stdin,
            tmp_dir,
            prefix,
        })
    }

    /// Run `command` in the persistent shell.
    ///
    /// - `input = Some(data)` — feeds `data` to the command's stdin via a fifo.
    /// - `input = None`       — stdin is /dev/null (used with --input-var).
    pub fn run(&mut self, command: &str, input: Option<&str>) -> io::Result<ShellResult> {
        let id = INVOCATION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let out = self.tmp_dir.join(format!("{}-{}-out", self.prefix, id));
        let err = self.tmp_dir.join(format!("{}-{}-err", self.prefix, id));
        let sig = self.tmp_dir.join(format!("{}-{}-sig", self.prefix, id));

        let out_s = shell_escape(out.to_str().unwrap());
        let err_s = shell_escape(err.to_str().unwrap());
        let sig_s = shell_escape(sig.to_str().unwrap());

        let wrapper = match input {
            Some(data) => {
                let fifo = self.tmp_dir.join(format!("{}-{}-in.fifo", self.prefix, id));
                let fifo_s = shell_escape(fifo.to_str().unwrap());

                let status = Command::new("mkfifo").arg(&fifo).status()?;
                if !status.success() {
                    return Err(io::Error::new(io::ErrorKind::Other, "mkfifo failed"));
                }

                let data_owned = data.to_owned();
                let fifo_writer = fifo.clone();
                std::thread::spawn(move || {
                    if let Ok(mut f) = fs::File::create(&fifo_writer) {
                        let _ = f.write_all(data_owned.as_bytes());
                    }
                    let _ = fs::remove_file(&fifo_writer);
                });

                format!(
                    "( {command} ) <{fifo_s} >{out_s} 2>{err_s}; echo $? >{sig_s}\n",
                    command = command,
                    fifo_s = fifo_s,
                    out_s = out_s,
                    err_s = err_s,
                    sig_s = sig_s,
                )
            }
            None => {
                format!(
                    "( {command} ) </dev/null >{out_s} 2>{err_s}; echo $? >{sig_s}\n",
                    command = command,
                    out_s = out_s,
                    err_s = err_s,
                    sig_s = sig_s,
                )
            }
        };

        if let Err(e) = self.stdin.write_all(wrapper.as_bytes()) {
            self.restart()?;
            return Err(e);
        }
        if let Err(e) = self.stdin.flush() {
            self.restart()?;
            return Err(e);
        }

        let exit_code = self.wait_for_signal(&sig)?;

        let stdout = fs::read_to_string(&out).unwrap_or_default();
        let stderr = fs::read_to_string(&err).unwrap_or_default();

        let _ = fs::remove_file(&out);
        let _ = fs::remove_file(&err);
        let _ = fs::remove_file(&sig);

        Ok(ShellResult {
            stdout,
            stderr,
            exit_code,
        })
    }

    fn wait_for_signal(&mut self, sig: &PathBuf) -> io::Result<i32> {
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => {
                    self.restart()?;
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "persistent shell exited unexpectedly",
                    ));
                }
                Ok(None) => {}
                Err(_) => {}
            }

            match fs::read_to_string(sig) {
                Ok(content) if !content.trim().is_empty() => {
                    return Ok(content.trim().parse::<i32>().unwrap_or(-1));
                }
                _ => {
                    std::thread::sleep(std::time::Duration::from_micros(200));
                }
            }
        }
    }

    fn restart(&mut self) -> io::Result<()> {
        let _ = self.child.kill();
        let _ = self.child.wait();

        let mut child = Command::new("sh")
            .arg("--norc")
            .arg("--noprofile")
            .arg("-u")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        self.stdin = child.stdin.take().expect("stdin was piped");
        self.child = child;
        Ok(())
    }
}

impl Drop for PersistentShell {
    fn drop(&mut self) {
        let _ = self.stdin.write_all(b"exit\n");
        let _ = self.stdin.flush();
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.tmp_dir);
    }
}

fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_command() {
        let mut sh = PersistentShell::new().unwrap();
        let r = sh.run("echo hello", None).unwrap();
        assert_eq!(r.stdout.trim(), "hello");
        assert_eq!(r.exit_code, 0);
    }

    #[test]
    fn exit_code_captured() {
        let mut sh = PersistentShell::new().unwrap();
        let r = sh.run("exit 42", None).unwrap();
        assert_eq!(r.exit_code, 42);
    }

    #[test]
    fn stderr_captured() {
        let mut sh = PersistentShell::new().unwrap();
        let r = sh.run("echo err >&2", None).unwrap();
        assert_eq!(r.stderr.trim(), "err");
        assert_eq!(r.exit_code, 0);
    }

    #[test]
    fn stdin_via_fifo() {
        let mut sh = PersistentShell::new().unwrap();
        let r = sh.run("cat", Some("hello from fifo")).unwrap();
        assert_eq!(r.stdout.trim(), "hello from fifo");
        assert_eq!(r.exit_code, 0);
    }

    #[test]
    fn stdin_multiline() {
        let mut sh = PersistentShell::new().unwrap();
        let r = sh.run("cat", Some("line1\nline2\nline3")).unwrap();
        let lines: Vec<&str> = r.stdout.trim().lines().collect();
        assert_eq!(lines, vec!["line1", "line2", "line3"]);
    }

    #[test]
    fn multiple_commands_sequential() {
        let mut sh = PersistentShell::new().unwrap();
        for i in 0..10 {
            let r = sh.run(&format!("echo {i}"), None).unwrap();
            assert_eq!(r.stdout.trim(), i.to_string());
        }
    }

    #[test]
    fn shell_survives_subshell_exit() {
        let mut sh = PersistentShell::new().unwrap();
        let r1 = sh.run("exit 1", None).unwrap();
        assert_eq!(r1.exit_code, 1);
        let r2 = sh.run("echo alive", None).unwrap();
        assert_eq!(r2.stdout.trim(), "alive");
    }

    #[test]
    fn env_var_no_stdin() {
        let mut sh = PersistentShell::new().unwrap();
        let r = sh.run("export FOO='bar'; echo $FOO", None).unwrap();
        assert_eq!(r.stdout.trim(), "bar");
    }
}
