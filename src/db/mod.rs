use std::path::PathBuf;

use rusqlite::{params, Connection};

use crate::{
    errors::Error,
    jobs::{Job, JobStatus},
};

pub struct DbBuilder(PathBuf);

impl DbBuilder {
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn build(self) -> Result<Db, Error> {
        let conn = Connection::open(&self.0)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS jobs (
                id        TEXT    PRIMARY KEY,
                command   TEXT    NOT NULL,
                input     TEXT    NOT NULL,
                start_time INTEGER,
                end_time   INTEGER,
                status    TEXT    NOT NULL,
                stdout    TEXT,
                stderr    TEXT,
                exit_code INTEGER,
                error     TEXT
            );",
        )?;
        Ok(Db { conn })
    }
}

pub struct Db {
    conn: Connection,
}

impl Db {
    /// Returns a cached `Job` if one exists for this id, or `None`.
    pub fn get(&self, id: &str) -> Result<Option<Job>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id, command, input, start_time, end_time,
                    status, stdout, stderr, exit_code, error
             FROM jobs WHERE id = ?1",
        )?;

        let result = stmt.query_row(params![id], |row| {
            let status_str: String = row.get(5)?;
            let status = match status_str.as_str() {
                "Pending" => JobStatus::Pending,
                "Running" => JobStatus::Running,
                "Done" => JobStatus::Done,
                _ => JobStatus::Errored,
            };
            Ok(Job {
                id: row.get(0)?,
                command: row.get(1)?,
                input: row.get(2)?,
                start_time: row.get::<_, Option<i64>>(3)?.map(|v| v as u128),
                end_time: row.get::<_, Option<i64>>(4)?.map(|v| v as u128),
                status,
                stdout: row.get(6)?,
                stderr: row.get(7)?,
                exit_code: row.get(8)?,
                error: row.get(9)?,
            })
        });

        match result {
            Ok(job) => Ok(Some(job)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(Error::Db(e)),
        }
    }

    /// Insert or replace a job in the cache.
    pub fn upsert(&self, job: &Job) -> Result<(), Error> {
        let status_str = match job.status {
            JobStatus::Pending => "Pending",
            JobStatus::Running => "Running",
            JobStatus::Done => "Done",
            JobStatus::Errored => "Errored",
        };
        self.conn.execute(
            "INSERT OR REPLACE INTO jobs
                 (id, command, input, start_time, end_time,
                  status, stdout, stderr, exit_code, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
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
            ],
        )?;
        Ok(())
    }
}
