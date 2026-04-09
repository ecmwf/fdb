//! Safe Rust wrapper for ECMWF's FDB (Fields `DataBase`).
//!
//! This crate provides a safe, idiomatic Rust interface to the FDB,
//! a domain-specific object store for meteorological data.
//!
//! # Example
//!
//! `list` accepts partial requests — any unset key matches everything — which
//! makes it the typical entry point for browsing what's archived.
//!
//! ```no_run
//! use fdb::{Fdb, Request};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let fdb = Fdb::new()?;
//!
//! let request = Request::new()
//!     .with("class", "od")
//!     .with("expver", "0001");
//!
//! // depth=3 for full traversal (db + index + datum); deduplicate=false
//! for item in fdb.list(&request, 3, false)? {
//!     let item = item?;
//!     let key = item
//!         .full_key()
//!         .into_iter()
//!         .map(|(k, v)| format!("{k}={v}"))
//!         .collect::<Vec<_>>()
//!         .join(",");
//!     println!("{{{key}}}");
//! }
//! # Ok(())
//! # }
//! ```

mod datareader;
mod error;
mod handle;
mod iterator;
mod key;
mod request;

pub use datareader::DataReader;
pub use error::{Error, Result};
pub use handle::{ArchiveCallbackData, Fdb, FdbStats};
pub use iterator::{
    ControlElement, ControlIterator, DumpElement, DumpIterator, ListElement, ListIterator,
    MoveElement, MoveIterator, PurgeElement, PurgeIterator, StatsElement, StatsIterator,
    StatusElement, StatusIterator, WipeElement, WipeIterator,
};
pub use key::Key;
pub use request::Request;

// Re-export control enums from the cxx bindings
pub use fdb_sys::{ControlAction, ControlIdentifier};

/// Version string of the underlying FDB C++ library.
#[must_use]
pub fn version() -> String {
    fdb_sys::fdb_version()
}

/// Git SHA1 of the underlying FDB C++ library.
#[must_use]
pub fn git_sha1() -> String {
    fdb_sys::fdb_git_sha1()
}
