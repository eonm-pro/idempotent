use std::time::Duration;

use crossbeam_channel::{select, tick, Receiver};
use log::{debug, info, warn};

use crate::db::Db;
use crate::errors::Error;
use crate::jobs::Job;

const BATCH_SIZE: usize = 64;
const BATCH_TIMEOUT: Duration = Duration::from_millis(200);

/// Drains completed jobs from `result_rx`, calls `on_job` for each, and
/// flushes them to the DB in batches.
///
/// Knows nothing about output formatting — that is the caller's concern
/// via the `on_job` callback.
pub struct Sink {
    db: Db,
    on_job: Box<dyn Fn(&Job) + Send>,
}

impl Sink {
    pub fn new(db: Db, on_job: impl Fn(&Job) + Send + 'static) -> Self {
        Self { db, on_job: Box::new(on_job) }
    }

    pub fn drain(self, result_rx: Receiver<Result<Job, Error>>) -> Result<(), Error> {
        let mut batch: Vec<Job> = Vec::with_capacity(BATCH_SIZE);
        let ticker = tick(BATCH_TIMEOUT);

        loop {
            select! {
                recv(result_rx) -> msg => {
                    match msg {
                        Ok(Ok(job)) => {
                            debug!("job {} finished ({:?})", &job.id[..8], job.status);
                            (self.on_job)(&job);
                            batch.push(job);
                            if batch.len() >= BATCH_SIZE {
                                self.flush(&mut batch)?;
                            }
                        }
                        Ok(Err(e)) => {
                            warn!("worker error: {e}");
                        }
                        Err(_) => {
                            info!("all workers done, flushing {} remaining jobs", batch.len());
                            self.flush(&mut batch)?;
                            return Ok(());
                        }
                    }
                }
                recv(ticker) -> _ => {
                    if !batch.is_empty() {
                        debug!("tick flush: {} jobs", batch.len());
                        self.flush(&mut batch)?;
                    }
                }
            }
        }
    }

    fn flush(&self, batch: &mut Vec<Job>) -> Result<(), Error> {
        debug!("flushing {} jobs to db", batch.len());
        self.db.upsert_batch(batch)?;
        batch.clear();
        Ok(())
    }
}
