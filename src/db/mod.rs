use std::path::PathBuf;

use rusqlite::{params, Connection, Transaction};

use crate::{
    errors::Error,
    jobs::{Job, JobStatus},
};

pub struct DbBuilder(PathBuf);

impl DbBuilder {
    pub fn new(path: PathBuf) -> Self { Self(path) }

    pub fn build(self) -> Result<Db, Error> {
        let conn = Connection::open(&self.0)?;

        // WAL mode: readers never block writers and writers never block readers.
        // synchronous=NORMAL: fsync only on WAL checkpoints, not every commit —
        // safe against OS crashes (WAL is durable), much faster than FULL.
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous  = NORMAL;
             PRAGMA cache_size   = -8000;  -- 8 MB page cache
             PRAGMA temp_store   = MEMORY;
             CREATE TABLE IF NOT EXISTS jobs (
                 id         TEXT    PRIMARY KEY,
                 command    TEXT    NOT NULL,
                 input      TEXT    NOT NULL,
                 start_time INTEGER,
                 end_time   INTEGER,
                 status     TEXT    NOT NULL,
                 stdout     TEXT,
                 stderr     TEXT,
                 exit_code  INTEGER,
                 error      TEXT
             );",
        )?;

        Ok(Db { conn })
    }
}

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn get(&self, id: &str) -> Result<Option<Job>, Error> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, command, input, start_time, end_time,
                    status, stdout, stderr, exit_code, error
             FROM jobs WHERE id = ?1",
        )?;

        let result = stmt.query_row(params![id], row_to_job);
        match result {
            Ok(job)                                   => Ok(Some(job)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e)                                    => Err(Error::Db(e)),
        }
    }

    /// Return all job IDs whose status is 'Done'. Used to build the in-process
    /// cache at startup so subsequent lookups cost zero SQLite round-trips.
    pub fn all_done_ids(&self) -> Result<std::collections::HashSet<String>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id FROM jobs WHERE status = 'Done'"
        )?;
        let ids = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        Ok(ids)
    }

    /// Return all cached jobs (for --cached-only display).
    pub fn all_jobs(&self) -> Result<Vec<Job>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id, command, input, start_time, end_time,
                    status, stdout, stderr, exit_code, error
             FROM jobs",
        )?;
        let jobs = stmt
            .query_map([], row_to_job)?
            .collect::<Result<_, _>>()?;
        Ok(jobs)
    }

    /// Insert or replace a single job. Prefer `upsert_batch` in hot paths.
    pub fn upsert(&self, job: &Job) -> Result<(), Error> {
        let mut stmt = self.conn.prepare_cached(
            "INSERT OR REPLACE INTO jobs
                 (id, command, input, start_time, end_time,
                  status, stdout, stderr, exit_code, error)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        )?;
        execute_job_stmt(&mut stmt, job)?;
        Ok(())
    }

    /// Write all jobs in `batch` inside **one transaction**.
    ///
    /// SQLite's real cost is the fsync on each commit. Batching N rows into
    /// one transaction means one fsync instead of N — a 10-100× speedup for
    /// small, fast jobs.
    pub fn upsert_batch(&self, batch: &[Job]) -> Result<(), Error> {
        if batch.is_empty() { return Ok(()); }

        let tx: Transaction = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO jobs
                     (id, command, input, start_time, end_time,
                      status, stdout, stderr, exit_code, error)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            )?;
            for job in batch {
                execute_job_stmt(&mut stmt, job)?;
            }
        }

        tx.commit()?;
        Ok(())
    }
}

fn row_to_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let status = match row.get::<_, String>(5)?.as_str() {
        "Pending" => JobStatus::Pending,
        "Running" => JobStatus::Running,
        "Done"    => JobStatus::Done,
        _         => JobStatus::Errored,
    };
    Ok(Job {
        id:         row.get(0)?,
        command:    row.get(1)?,
        input:      row.get(2)?,
        input_var: None,
        start_time: row.get::<_, Option<i64>>(3)?.map(|v| v as u128),
        end_time:   row.get::<_, Option<i64>>(4)?.map(|v| v as u128),
        status,
        stdout:     row.get(6)?,
        stderr:     row.get(7)?,
        exit_code:  row.get(8)?,
        error:      row.get(9)?,
    })
}

/// Execute a pre-prepared INSERT OR REPLACE statement for `job`.
///
/// We can't extract `params![...]` into a helper that returns `impl Params`
/// because `params!` borrows its arguments, and a local `&str` for
/// `status_str` would not live long enough to be returned. Passing the
/// statement in and calling `execute` immediately sidesteps the lifetime.
fn execute_job_stmt(
    stmt: &mut rusqlite::Statement<'_>,
    job: &Job,
) -> rusqlite::Result<()> {
    let status_str = match job.status {
        JobStatus::Pending => "Pending",
        JobStatus::Running => "Running",
        JobStatus::Done    => "Done",
        JobStatus::Errored => "Errored",
    };
    stmt.execute(params![
        job.id,
        job.command,
        job.input,
        job.start_time.map(|v| v as i64),
        job.end_time.map(|v| v as i64),
        status_str,
        job.stdout,
        job.stderr,
        job.exit_code,
        job.error,
    ])?;
    Ok(())
}