use clap::Parser;
use std::path::PathBuf;

use crate::input::Input;

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Cli {
    /// Shell command to run for each input chunk (passed to `sh -c`)
    #[arg(short, long)]
    pub command: String,

    /// Path to the SQLite cache database
    #[arg(short, long, default_value = ".memorun.db")]
    pub db: PathBuf,

    /// Read input from stdin
    #[arg(long, conflicts_with = "file")]
    pub stdin: bool,

    /// Read input from file
    #[arg(long)]
    pub file: Option<PathBuf>,

    /// Print cached results without re-running the command
    #[arg(long)]
    pub cached_only: bool,

    /// Don't output any information to stdout
    #[arg(short, long)]
    pub silent: bool,

    /// Force re-run even if a cached result exists
    #[arg(long, short)]
    pub force: bool,
}

impl Cli {
    pub fn input(&self) -> Input {
        match (self.stdin, &self.file) {
            (true, None) => Input::Stdin,
            (false, Some(path)) => Input::File(path.clone()),

            // default if nothing specified (you can change this if you want strictness)
            (false, None) => Input::Stdin,

            // clap should prevent this, but don't trust it blindly
            (true, Some(_)) => unreachable!("clap enforces conflicts"),
        }
    }
}
