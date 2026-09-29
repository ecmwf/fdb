//! Error handling for FDB.

/// Error type for FDB operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error from eckit/metkit C++ libraries.
    #[error(transparent)]
    Eckit(#[from] eckit::Error),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Data size exceeds platform capacity.
    #[error("data size exceeds platform capacity: {0}")]
    SizeOverflow(#[from] std::num::TryFromIntError),
}

/// Result type alias for FDB operations.
pub type Result<T> = std::result::Result<T, Error>;

impl From<fdb_sys::Exception> for Error {
    fn from(e: fdb_sys::Exception) -> Self {
        Self::Eckit(eckit::Error::from(e))
    }
}
