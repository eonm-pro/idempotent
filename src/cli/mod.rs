use clap::Parser;
use std::path::PathBuf;

use crate::input::Input;

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Cli {
    /// Shell command to run for each input line (passed to `sh -c`)
    #[arg(short, long)]
    pub command: String,

    /// Path to the SQLite cache database
    #[arg(short, long, default_value = ".memorun.db")]
    pub db: PathBuf,

    /// Read input from stdin
    #[arg(long, conflicts_with = "file")]
    pub stdin: bool,

    /// Read input from a file (one job per line)
    #[arg(long)]
    pub file: Option<PathBuf>,

    /// Print cached results without re-running the command
    #[arg(long)]
    pub cached_only: bool,

    /// Suppress stdout output (errors still go to stderr)
    #[arg(short, long)]
    pub silent: bool,

    /// Force re-run even if a cached result exists
    #[arg(long, short)]
    pub force: bool,

    /// Number of parallel jobs [default: logical CPU count]
    #[arg(
        short = 'j',
        long,
        default_value_t = default_parallelism(),
    )]
    pub jobs: usize,

    /// Expose the input line as an env var in the child process (e.g. --input-var LINE)
    #[arg(long)]
    pub input_var: Option<String>,
}

fn default_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

impl Cli {
    pub fn input(&self) -> Input {
        match (self.stdin, &self.file) {
            (true, None)   => Input::Stdin,
            (false, Some(path)) => Input::File(path.clone()),
            (false, None)  => Input::Stdin, // default when nothing specified
            (true, Some(_)) => unreachable!("clap enforces conflicts_with"),
        }
    }
}