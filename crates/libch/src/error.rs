/// Why a libch call failed outright. An operation that ran and committed nothing for a reason the caller can act
/// on reports an [`Outcome`](crate::Outcome) instead.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("InodeDB: {0}")]
    InodeDb(#[from] inodedb::Error),

    /// An exception a ClickHouse server answered with.
    #[error("ClickHouse exception {code}: {message}")]
    ClickHouse { code: i32, message: String },

    #[error("HTTP: {0}")]
    Http(#[from] reqwest::Error),

    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("S3: {0}")]
    S3(String),

    /// A value, record, definition or coordination file that libch cannot read.
    #[error("{path}: {message}")]
    Format { path: String, message: String },

    /// A ClickHouse server process that failed to start, answer or stop.
    #[error("server {name}: {message}")]
    Server { name: String, message: String },

    /// A worker process that failed to start, to finish, or to answer as the protocol says.
    #[error("worker: {message}")]
    Worker { message: String },

    #[error("{0}")]
    Other(String),
}

impl Error {
    pub(crate) fn format(path: impl Into<String>, message: impl Into<String>) -> Error {
        Error::Format { path: path.into(), message: message.into() }
    }

    /// The ClickHouse exception code, when a server answered with one.
    pub fn clickhouse_code(&self) -> Option<i32> {
        match self {
            Error::ClickHouse { code, .. } => Some(*code),
            _ => None,
        }
    }
}
