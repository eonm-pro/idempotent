mod cli;
mod db;
mod errors;
mod input;
mod jobs;
mod tasks;

use std::io::BufRead;

use crate::jobs::Job;

use clap::Parser;
use db::DbBuilder;

fn main() -> Result<(), errors::Error> {
    let cli = cli::Cli::parse();

    let input = cli.input().buf_reader()?;
    let command = cli.command;
    let db = DbBuilder::new(cli.db).build()?;

    let lines = input.lines();

    for line in lines {
        let mut job = Job::new(command.clone(), line?.to_string());

        match db.get(&job.id)? {
            Some(_job) => (),
            None => {
                job = job.run();
            }
        }
    }

    Ok(())
}
