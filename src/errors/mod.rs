use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("shell error: {0}")]
    Shell(String),

    #[error("worker pool shut down unexpectedly")]
    PoolShutdown,

    #[error("sink thread panicked")]
    SinkPanicked,
}
