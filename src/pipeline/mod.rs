use crossbeam_channel::bounded;
use log::info;

use crate::db::Db;
use crate::errors::Error;
use crate::jobs::Job;
use crate::runner;
use crate::sink::Sink;

/// Wires the worker pool and the sink together.
///
/// Accepts an `on_job` callback so callers decide what to do with each
/// completed job — the pipeline itself has no opinion on output formatting.
pub struct Pipeline {
    work_tx: crossbeam_channel::Sender<Job>,
    sink_thread: std::thread::JoinHandle<Result<(), Error>>,
}

impl Pipeline {
    pub fn new(
        parallelism: usize,
        db: Db,
        on_job: impl Fn(&Job) + Send + 'static,
    ) -> Self {
        let (work_tx, work_rx) = bounded(parallelism * 4);
        let (result_tx, result_rx) = bounded(parallelism * 4);

        info!("starting pipeline with {parallelism} workers");
        runner::spawn(parallelism, work_rx, result_tx);

        let sink_thread = std::thread::spawn(move || {
            Sink::new(db, on_job).drain(result_rx)
        });

        Self { work_tx, sink_thread }
    }

    pub fn submit(&self, job: Job) -> Result<(), Error> {
        self.work_tx.send(job).map_err(|_| Error::PoolShutdown)
    }

    /// Signal shutdown and block until all jobs are flushed to the DB.
    pub fn wait(self) -> Result<(), Error> {
        info!("closing pipeline");
        drop(self.work_tx);
        self.sink_thread.join().map_err(|_| Error::SinkPanicked)?
    }
}
