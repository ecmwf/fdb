//! Error handling for FDB.
//!
//! The FDB's own exception classes are variants of [`Error`]; eckit
//! exceptions that surface through the same bridge are nested as
//! [`Error::Eckit`]. A `cxx::Exception` whose message matches neither
//! namespace is preserved as [`Error::Other`].
//!
//! # Example
//!
//! ```no_run
//! use fdb::{Error, Fdb};
//!
//! let config: eckit::Config = "type: local\n".parse()?;
//! match Fdb::open(Some(&config), None) {
//!     Ok(fdb) => println!("{}", fdb.name()),
//!     Err(Error::SchemaError(msg)) => eprintln!("bad schema: {msg}"),
//!     Err(Error::Eckit(eckit::Error::CantOpenFile(msg))) => eprintln!("cannot open: {msg}"),
//!     Err(other) => eprintln!("{other}"),
//! }
//! # Ok::<(), fdb::Error>(())
//! ```

/// Error type for FDB operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// `fdb5::SchemaError`: the schema file failed to parse.
    #[error("fdb::SchemaError: {0}")]
    SchemaError(String),

    /// Typed eckit exception (e.g. `UserError`, `SeriousBug`, `CantOpenFile`).
    /// The remote client's exceptions arrive here as `RemoteException`.
    #[error(transparent)]
    Eckit(#[from] eckit::Error),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Data size exceeds platform capacity.
    #[error("data size exceeds platform capacity: {0}")]
    SizeOverflow(#[from] std::num::TryFromIntError),

    /// Exception from neither namespace; raw message preserved.
    #[error("{0}")]
    Other(String),
}

/// Result type alias for FDB operations.
pub type Result<T> = std::result::Result<T, Error>;

impl From<fdb_sys::Error> for Error {
    fn from(e: fdb_sys::Error) -> Self {
        match e {
            fdb_sys::Error::SchemaError(msg) => Self::SchemaError(msg),
            fdb_sys::Error::Other(msg) => Self::Other(msg),
        }
    }
}

impl From<fdb_sys::Exception> for Error {
    fn from(e: fdb_sys::Exception) -> Self {
        if let Some(fdb) = fdb_sys::Error::try_from_cxx(&e) {
            return Self::from(fdb);
        }
        if let Some(eckit) = eckit::Error::try_from_cxx(&e) {
            return Self::Eckit(eckit);
        }
        Self::Other(e.what().to_string())
    }
}
