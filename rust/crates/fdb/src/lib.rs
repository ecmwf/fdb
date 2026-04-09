//! Safe Rust wrapper for ECMWF's FDB (Fields DataBase).
//!
//! This crate provides a safe, idiomatic Rust interface to the FDB,
//! a domain-specific object store for meteorological data.
//!
//! # Example
//!
//! ```no_run
//! use fdb::{Fdb, Request};
//!
//! let mut fdb = Fdb::new().expect("failed to create FDB handle");
//!
//! // Create a request for listing data
//! let request = Request::new()
//!     .with("class", "od")
//!     .with("expver", "0001");
//!
//! // List matching fields (depth=3 for full traversal, no duplicates)
//! for item in fdb.list(&request, 3, false).expect("list failed") {
//!     let item = item.expect("failed to get item");
//!     println!("Found: {} (offset={}, length={})", item.uri, item.offset, item.length);
//! }
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
