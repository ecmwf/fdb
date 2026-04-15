//! C++ bindings to ECMWF's FDB (Fields `DataBase`) library using cxx.
//!
//! This crate provides raw C++ bindings to the FDB. For a safe, idiomatic
//! Rust interface, use the `fdb` crate instead.

#![allow(clippy::needless_lifetimes)]
#![allow(clippy::must_use_candidate)]

use bindman::track_cpp_api;

/// Data passed to archive callbacks.
#[derive(Debug, Clone)]
pub struct ArchiveCallbackData {
    /// The key entries for the archived data.
    pub key: Vec<(String, String)>,
    /// The archived data.
    pub data: Vec<u8>,
    /// Field location URI (available after write completes).
    pub location_uri: Option<String>,
    /// Field location offset.
    pub location_offset: u64,
    /// Field location length.
    pub location_length: u64,
}

/// Trait for flush callbacks.
pub trait FlushCallback: Send {
    fn on_flush(&self);
}

/// Trait for archive callbacks.
pub trait ArchiveCallback: Send {
    fn on_archive(&self, data: ArchiveCallbackData);
}

// Box wrappers for the callbacks (so they can be stored as opaque types)
/// Opaque wrapper for flush callbacks (used internally by cxx bridge).
pub struct FlushCallbackBox(Box<dyn FlushCallback>);
/// Opaque wrapper for archive callbacks (used internally by cxx bridge).
pub struct ArchiveCallbackBox(Box<dyn ArchiveCallback>);

/// Opaque wrapper for an arbitrary Rust [`std::io::Read`] source.
///
/// Exposed to the C++ side as an `eckit::DataHandle` by
/// [`archive_reader`] to stream GRIB data from a Rust source into FDB
/// without buffering the entire payload in memory first.
pub struct ReaderBox(Box<dyn std::io::Read + Send>);

// Methods intentionally not exposed:
// - `axesIterator`: internal detail of the multi-FDB implementation
//   (DistFDB / SelectFDB), not meaningful at the user API. The synchronous
//   `axes()` method is the supported entry point.
// - `config`: returns the same configuration the user just supplied to
//   `Fdb::from_yaml(...)`. The user already has it; round-tripping it back
//   through the FFI adds no information.
// - `move`: admin-tier operation for physically relocating FDB databases
//   between storage roots. Upstream `fdb-move` drives an MPI-based
//   producer/consumer transport and calls `FileCopy::execute` / `cleanup`
//   per element — none of which is feasible to bind cleanly, and none of
//   which pyfdb exposes either. Rust programs that need to relocate data
//   should shell out to the `fdb-move` CLI tool.
#[track_cpp_api(
    "fdb5/api/FDB.h",
    class = "FDB",
    ignore = ["inspect", "reindex", "axesIterator", "config", "move"]
)]
#[cxx::bridge(namespace = "fdb::ffi")]
mod ffi {
    // =========================================================================
    // Shared structs (POD-like types that can cross the FFI boundary)
    // =========================================================================

    /// A key/value pair for FDB metadata.
    #[derive(Debug, Clone, Default)]
    pub struct KeyValue {
        pub key: String,
        pub value: String,
    }

    /// Data for constructing an FDB Key.
    #[derive(Debug, Clone, Default)]
    pub struct KeyData {
        pub entries: Vec<KeyValue>,
    }

    /// A single key in a parsed MARS request, paired with all of its values.
    #[derive(Debug, Clone, Default)]
    pub struct RequestParam {
        pub key: String,
        pub values: Vec<String>,
    }

    /// A fully-expanded MARS request, as produced by `parse_mars_request`.
    /// `to`/`by` ranges, type expansions, etc. have already been applied by
    /// `metkit::mars::MarsExpansion`.
    #[derive(Debug, Clone, Default)]
    pub struct RequestData {
        pub params: Vec<RequestParam>,
    }

    /// Data returned from list iteration.
    #[derive(Debug, Clone, Default)]
    pub struct ListElementData {
        /// URI of the data location
        pub uri: String,
        /// Offset within the file
        pub offset: u64,
        /// Length of the data
        pub length: u64,
        /// Database key entries
        pub db_key: Vec<KeyValue>,
        /// Index key entries
        pub index_key: Vec<KeyValue>,
        /// Datum key entries
        pub datum_key: Vec<KeyValue>,
        /// Timestamp (Unix epoch seconds)
        pub timestamp: i64,
    }

    /// An axis entry (key -> values mapping).
    #[derive(Debug, Clone, Default)]
    pub struct AxisEntry {
        pub key: String,
        pub values: Vec<String>,
    }

    /// Aggregate FDB statistics.
    #[derive(Debug, Clone, Default)]
    pub struct FdbStatsData {
        /// Number of archive operations
        pub num_archive: u64,
        /// Number of location operations
        pub num_location: u64,
        /// Number of flush operations
        pub num_flush: u64,
    }

    /// Result from dump iteration.
    #[derive(Debug, Clone, Default)]
    pub struct DumpElementData {
        /// String representation of the dump element
        pub content: String,
    }

    /// Result from status iteration.
    #[derive(Debug, Clone, Default)]
    pub struct StatusElementData {
        /// Path/location
        pub location: String,
        /// Status information as key-value pairs
        pub status: Vec<KeyValue>,
    }

    /// Result from wipe iteration.
    #[derive(Debug, Clone, Default)]
    pub struct WipeElementData {
        /// String representation of wiped element
        pub content: String,
    }

    /// Result from purge iteration.
    #[derive(Debug, Clone, Default)]
    pub struct PurgeElementData {
        /// String representation of purged element
        pub content: String,
    }

    /// Internal transport for `list_iterator_dump_compact`. Mirrors
    /// what `fdb5::ListIterator::dumpCompact` produces: aggregated
    /// MARS-request text plus the two counters it returns. The
    /// high-level `ListIterator::dump_compact` immediately writes
    /// `text` into the caller's `std::io::Write` and drops this struct,
    /// so the `text` allocation is bridge-internal.
    #[derive(Debug, Clone, Default)]
    pub struct CompactListingData {
        pub text: String,
        pub fields: u64,
        pub total_bytes: u64,
    }

    /// Index-level stats — mirrors `fdb5::IndexStats`. Bundles the four
    /// numeric accessors (`fieldsCount` / `fieldsSize` /
    /// `duplicatesCount` / `duplicatesSize`) plus the `report()` text.
    #[derive(Debug, Clone, Default)]
    pub struct IndexStatsData {
        pub fields_count: u64,
        pub fields_size: u64,
        pub duplicates_count: u64,
        pub duplicates_size: u64,
        /// Captured `fdb5::IndexStats::report()` output.
        pub report: String,
    }

    /// Database-level stats — mirrors `fdb5::DbStats`. Upstream exposes
    /// `DbStats` as fully opaque content; the only public read accessor
    /// is `report(std::ostream&)`, so the captured report text is the
    /// only thing we can surface.
    #[derive(Debug, Clone, Default)]
    pub struct DbStatsData {
        /// Captured `fdb5::DbStats::report()` output.
        pub report: String,
    }

    /// Result from stats iteration — mirrors `fdb5::StatsElement`.
    #[derive(Debug, Clone, Default)]
    pub struct StatsElementData {
        pub index_statistics: IndexStatsData,
        pub db_statistics: DbStatsData,
    }

    /// Result from control iteration.
    #[derive(Debug, Clone, Default)]
    pub struct ControlElementData {
        /// Location
        pub location: String,
        /// Control identifiers (each variant is the same as `fdb5::ControlIdentifier`).
        pub identifiers: Vec<ControlIdentifier>,
    }

    // Bind to existing fdb5::ControlAction / fdb5::ControlIdentifier C++ enums.
    // The shared enum + extern type pattern tells CXX to use the existing
    // C++ enum and generate static assertions to verify the values match.
    /// Control action for database features.
    #[namespace = "fdb5"]
    #[repr(u16)]
    pub enum ControlAction {
        /// No action (query current state).
        None = 0,
        /// Disable the feature.
        Disable = 1,
        /// Enable the feature.
        Enable = 2,
    }

    /// Feature identifier for `control()` operations. Bitflag values match
    /// `fdb5::ControlIdentifier` exactly.
    #[namespace = "fdb5"]
    #[repr(u16)]
    #[derive(Debug)]
    pub enum ControlIdentifier {
        None = 0,
        List = 1,
        Retrieve = 2,
        Archive = 4,
        Wipe = 8,
        UniqueRoot = 16,
    }

    #[namespace = "fdb5"]
    unsafe extern "C++" {
        include!("fdb5/api/helpers/ControlIterator.h");
        type ControlAction;
        type ControlIdentifier;
    }

    // =========================================================================
    // C++ types and functions
    // =========================================================================

    unsafe extern "C++" {
        include!("fdb_bridge.h");

        // =====================================================================
        // FdbHandle - Main FDB handle
        // =====================================================================

        /// Wrapper around fdb5::FDB
        type FdbHandle;

        /// Check if the FDB has unflushed data.
        fn dirty(self: &FdbHandle) -> bool;

        /// Flush pending writes to disk.
        fn flush(self: Pin<&mut FdbHandle>) -> Result<()>;

        /// Get aggregate statistics for the FDB handle.
        fn stats(self: &FdbHandle) -> FdbStatsData;

        /// Check if a control identifier is enabled.
        fn enabled(self: &FdbHandle, identifier: ControlIdentifier) -> bool;

        /// Get the FDB configuration ID.
        fn id(self: &FdbHandle) -> String;

        /// Get the FDB type name (e.g., "local", "remote").
        fn name(self: &FdbHandle) -> String;

        // =====================================================================
        // eckit::DataHandle - For reading retrieved data
        // =====================================================================

        /// Opaque handle to an `eckit::DataHandle` (the upstream abstract
        /// base for byte streams). Owned via `UniquePtr<DataHandle>`;
        /// `eckit::DataHandle` has a virtual destructor so cxx's
        /// generated `delete` is correct for any concrete subclass.
        #[namespace = "eckit"]
        type DataHandle;

        /// Open the handle for reading. Returns the estimated length.
        fn data_handle_open(handle: Pin<&mut DataHandle>) -> Result<u64>;

        /// Close the handle.
        fn data_handle_close(handle: Pin<&mut DataHandle>) -> Result<()>;

        /// Read up to `buffer.len()` bytes into `buffer`.
        fn data_handle_read(handle: Pin<&mut DataHandle>, buffer: &mut [u8]) -> Result<usize>;

        /// Seek to an absolute byte position.
        fn data_handle_seek(handle: Pin<&mut DataHandle>, position: u64) -> Result<()>;

        /// Current read position.
        fn data_handle_tell(handle: Pin<&mut DataHandle>) -> u64;

        /// Total size of the underlying data, in bytes.
        fn data_handle_size(handle: Pin<&mut DataHandle>) -> u64;

        // =====================================================================
        // ListIteratorHandle
        // =====================================================================

        /// Wrapper around fdb5::ListIterator
        type ListIteratorHandle;

        /// Check if the iterator has more elements.
        fn hasNext(self: Pin<&mut ListIteratorHandle>) -> Result<bool>;

        /// Get the next element from the iterator.
        fn next(self: Pin<&mut ListIteratorHandle>) -> Result<ListElementData>;

        /// Drain the iterator via `fdb5::ListIterator::dumpCompact`,
        /// returning the aggregated MARS-request text and the two
        /// counters. Mirrors `fdb-list --compact`.
        fn list_iterator_dump_compact(
            iterator: Pin<&mut ListIteratorHandle>,
        ) -> Result<CompactListingData>;

        // =====================================================================
        // DumpIteratorHandle
        // =====================================================================

        /// Wrapper around fdb5::DumpIterator
        type DumpIteratorHandle;

        /// Check if the iterator has more elements.
        fn hasNext(self: Pin<&mut DumpIteratorHandle>) -> Result<bool>;

        /// Get the next element from the iterator.
        fn next(self: Pin<&mut DumpIteratorHandle>) -> Result<DumpElementData>;

        // =====================================================================
        // StatusIteratorHandle
        // =====================================================================

        /// Wrapper around fdb5::StatusIterator
        type StatusIteratorHandle;

        /// Check if the iterator has more elements.
        fn hasNext(self: Pin<&mut StatusIteratorHandle>) -> Result<bool>;

        /// Get the next element from the iterator.
        fn next(self: Pin<&mut StatusIteratorHandle>) -> Result<StatusElementData>;

        // =====================================================================
        // WipeIteratorHandle
        // =====================================================================

        /// Wrapper around fdb5::WipeIterator
        type WipeIteratorHandle;

        /// Check if the iterator has more elements.
        fn hasNext(self: Pin<&mut WipeIteratorHandle>) -> Result<bool>;

        /// Get the next element from the iterator.
        fn next(self: Pin<&mut WipeIteratorHandle>) -> Result<WipeElementData>;

        // =====================================================================
        // PurgeIteratorHandle
        // =====================================================================

        /// Wrapper around fdb5::PurgeIterator
        type PurgeIteratorHandle;

        /// Check if the iterator has more elements.
        fn hasNext(self: Pin<&mut PurgeIteratorHandle>) -> Result<bool>;

        /// Get the next element from the iterator.
        fn next(self: Pin<&mut PurgeIteratorHandle>) -> Result<PurgeElementData>;

        // =====================================================================
        // StatsIteratorHandle
        // =====================================================================

        /// Wrapper around fdb5::StatsIterator
        type StatsIteratorHandle;

        /// Check if the iterator has more elements.
        fn hasNext(self: Pin<&mut StatsIteratorHandle>) -> Result<bool>;

        /// Get the next element from the iterator.
        fn next(self: Pin<&mut StatsIteratorHandle>) -> Result<StatsElementData>;

        // =====================================================================
        // ControlIteratorHandle
        // =====================================================================

        /// Wrapper around fdb5::ControlIterator
        type ControlIteratorHandle;

        /// Check if the iterator has more elements.
        fn hasNext(self: Pin<&mut ControlIteratorHandle>) -> Result<bool>;

        /// Get the next element from the iterator.
        fn next(self: Pin<&mut ControlIteratorHandle>) -> Result<ControlElementData>;

        // =====================================================================
        // Initialization (free functions)
        // =====================================================================

        /// Initialize the FDB library.
        /// Must be called before any other FDB operations.
        fn fdb_init();

        // =====================================================================
        // Library metadata (free functions)
        // =====================================================================

        /// Get the FDB library version string.
        fn fdb_version() -> String;

        /// Get the FDB git SHA1 hash.
        fn fdb_git_sha1() -> String;

        // =====================================================================
        // MARS request parsing (free functions)
        // =====================================================================

        /// Parse a MARS request string using metkit's parser and expansion
        /// machinery. Handles `to`/`by` ranges, type expansion, optional
        /// fields, and any other syntax the upstream MARS language supports.
        ///
        /// On success, returns the fully-expanded request as a sequence of
        /// `(key, [values])` pairs. On parse failure, returns an `Err` whose
        /// message comes from the underlying eckit/metkit exception.
        fn parse_mars_request(request: &str) -> Result<RequestData>;

        // =====================================================================
        // Handle lifecycle (free functions)
        // =====================================================================

        /// Create a new FDB handle with default configuration.
        fn new_fdb() -> Result<UniquePtr<FdbHandle>>;

        /// Create a new FDB handle from YAML configuration.
        fn new_fdb_from_yaml(config: &str) -> Result<UniquePtr<FdbHandle>>;

        /// Create a new FDB handle from YAML configuration plus a YAML
        /// per-instance "user config" (e.g. `useSubToc`, `preloadTocBTree`).
        fn new_fdb_from_yaml_with_user_config(
            config: &str,
            user_config: &str,
        ) -> Result<UniquePtr<FdbHandle>>;

        /// Create a new FDB handle by loading the configuration file at
        /// `path`. Delegates to `fdb5::Config::make`, which loads YAML or
        /// JSON, expands `~fdb` and `fdb_home` references, and resolves
        /// transitive sub-configurations.
        fn new_fdb_from_path(path: &str) -> Result<UniquePtr<FdbHandle>>;

        /// Same as `new_fdb_from_path` but additionally applies a YAML
        /// per-instance "user config" (e.g. `useSubToc`).
        fn new_fdb_from_path_with_user_config(
            path: &str,
            user_config: &str,
        ) -> Result<UniquePtr<FdbHandle>>;

        // =====================================================================
        // Archive operations (free functions)
        // =====================================================================

        /// Archive data with an explicit key.
        fn archive(handle: Pin<&mut FdbHandle>, key: &KeyData, data: &[u8]) -> Result<()>;

        /// Archive raw GRIB data (key is extracted from the message).
        fn archive_raw(handle: Pin<&mut FdbHandle>, data: &[u8]) -> Result<()>;

        /// Archive raw GRIB data streamed from an arbitrary Rust
        /// `std::io::Read` source. The C++ side wraps the [`ReaderBox`]
        /// in an `eckit::DataHandle` subclass and hands it to
        /// `fdb5::FDB::archive(eckit::DataHandle&)`, which extracts the
        /// metadata from each GRIB message as it streams.
        fn archive_reader(handle: Pin<&mut FdbHandle>, reader: Box<ReaderBox>) -> Result<()>;

        // =====================================================================
        // Retrieve operations (free functions)
        // =====================================================================

        /// Retrieve data matching a request.
        fn retrieve(handle: Pin<&mut FdbHandle>, request: &str) -> Result<UniquePtr<DataHandle>>;

        // =====================================================================
        // Read operations (by URI)
        // =====================================================================

        /// Read data from a single URI.
        fn read_uri(handle: Pin<&mut FdbHandle>, uri: &str) -> Result<UniquePtr<DataHandle>>;

        /// Read data from a list of URIs.
        fn read_uris(
            handle: Pin<&mut FdbHandle>,
            uris: &Vec<String>,
            in_storage_order: bool,
        ) -> Result<UniquePtr<DataHandle>>;

        /// Read data from a list iterator (most efficient).
        fn read_list_iterator(
            handle: Pin<&mut FdbHandle>,
            iterator: Pin<&mut ListIteratorHandle>,
            in_storage_order: bool,
        ) -> Result<UniquePtr<DataHandle>>;

        // =====================================================================
        // List operations (free functions)
        // =====================================================================

        /// List data matching a request.
        fn list(
            handle: Pin<&mut FdbHandle>,
            request: &str,
            deduplicate: bool,
            level: i32,
        ) -> Result<UniquePtr<ListIteratorHandle>>;

        // =====================================================================
        // Axes query (free functions)
        // =====================================================================

        /// Get axes (available metadata dimensions) for a request.
        fn axes(handle: Pin<&mut FdbHandle>, request: &str, level: i32) -> Result<Vec<AxisEntry>>;

        // =====================================================================
        // Dump operations (free functions)
        // =====================================================================

        /// Dump database structure.
        fn dump(
            handle: Pin<&mut FdbHandle>,
            request: &str,
            simple: bool,
        ) -> Result<UniquePtr<DumpIteratorHandle>>;

        // =====================================================================
        // Status operations (free functions)
        // =====================================================================

        /// Get database status.
        fn status(
            handle: Pin<&mut FdbHandle>,
            request: &str,
        ) -> Result<UniquePtr<StatusIteratorHandle>>;

        // =====================================================================
        // Wipe operations (free functions)
        // =====================================================================

        /// Wipe (delete) data matching a request.
        fn wipe(
            handle: Pin<&mut FdbHandle>,
            request: &str,
            doit: bool,
            porcelain: bool,
            unsafe_wipe_all: bool,
        ) -> Result<UniquePtr<WipeIteratorHandle>>;

        // =====================================================================
        // Purge operations (free functions)
        // =====================================================================

        /// Purge duplicate data.
        fn purge(
            handle: Pin<&mut FdbHandle>,
            request: &str,
            doit: bool,
            porcelain: bool,
        ) -> Result<UniquePtr<PurgeIteratorHandle>>;

        // =====================================================================
        // Stats operations (free functions)
        // =====================================================================

        /// Get statistics iterator.
        fn stats_iterator(
            handle: Pin<&mut FdbHandle>,
            request: &str,
        ) -> Result<UniquePtr<StatsIteratorHandle>>;

        // =====================================================================
        // Control operations (free functions)
        // =====================================================================

        /// Control database features.
        fn control(
            handle: Pin<&mut FdbHandle>,
            request: &str,
            action: ControlAction,
            identifiers: &[ControlIdentifier],
        ) -> Result<UniquePtr<ControlIteratorHandle>>;

        // =====================================================================
        // Callback registration (free functions)
        // =====================================================================

        /// Register a flush callback.
        /// The callback will be invoked when flush() is called.
        fn register_flush_callback(handle: Pin<&mut FdbHandle>, callback: Box<FlushCallbackBox>);

        /// Register an archive callback.
        /// The callback will be invoked for each field archived.
        fn register_archive_callback(
            handle: Pin<&mut FdbHandle>,
            callback: Box<ArchiveCallbackBox>,
        );

        // =====================================================================
        // Test functions (for verifying exception handling)
        // =====================================================================

        /// Test function that throws eckit::Exception
        fn test_throw_eckit_exception() -> Result<()>;

        /// Test function that throws eckit::SeriousBug
        fn test_throw_eckit_serious_bug() -> Result<()>;

        /// Test function that throws eckit::UserError
        fn test_throw_eckit_user_error() -> Result<()>;

        /// Test function that throws std::runtime_error
        fn test_throw_std_exception() -> Result<()>;

        /// Test function that throws an int (non-std::exception type)
        fn test_throw_int() -> Result<()>;
    }

    // =========================================================================
    // Rust types exposed to C++
    // =========================================================================

    extern "Rust" {
        type FlushCallbackBox;
        type ArchiveCallbackBox;
        type ReaderBox;

        /// Called by C++ to invoke the flush callback.
        fn invoke_flush_callback(callback: &FlushCallbackBox);

        /// Called by C++ to invoke the archive callback.
        fn invoke_archive_callback(
            callback: &ArchiveCallbackBox,
            key: &[KeyValue],
            data: &[u8],
            location_uri: &str,
            location_offset: u64,
            location_length: u64,
        );

        /// Called by C++ to read the next chunk from a Rust `Read` source
        /// that has been wrapped in a [`ReaderBox`]. Returns the number of
        /// bytes read on success (0 means EOF), or `-1` if the underlying
        /// reader returned an error or panicked.
        fn invoke_reader_read(reader: &mut ReaderBox, buf: &mut [u8]) -> i64;
    }
}

// =============================================================================
// Callback invocation functions (called from C++)
// =============================================================================

fn invoke_flush_callback(callback: &FlushCallbackBox) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        callback.0.on_flush();
    }))
    .is_err()
    {
        eprintln!("fdb-sys: panic in flush callback (suppressed at FFI boundary)");
    }
}

fn invoke_archive_callback(
    callback: &ArchiveCallbackBox,
    key: &[ffi::KeyValue],
    data: &[u8],
    location_uri: &str,
    location_offset: u64,
    location_length: u64,
) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let key_vec: Vec<(String, String)> = key
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();

        let callback_data = ArchiveCallbackData {
            key: key_vec,
            data: data.to_vec(),
            location_uri: if location_uri.is_empty() {
                None
            } else {
                Some(location_uri.to_string())
            },
            location_offset,
            location_length,
        };

        callback.0.on_archive(callback_data);
    }))
    .is_err()
    {
        eprintln!("fdb-sys: panic in archive callback (suppressed at FFI boundary)");
    }
}

/// Called by the C++ `RustReaderHandle::read` shim to fill the next chunk
/// from a Rust [`std::io::Read`] source. Returns the byte count on success
/// (0 = EOF), or `-1` on error/panic, mirroring the convention used by
/// `eckit::DataHandle::read`.
fn invoke_reader_read(reader: &mut ReaderBox, buf: &mut [u8]) -> i64 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.0.read(buf)));
    match result {
        Ok(Ok(n)) => i64::try_from(n).unwrap_or(i64::MAX),
        Ok(Err(e)) => {
            eprintln!("fdb-sys: error reading from Rust source: {e}");
            -1
        }
        Err(_) => {
            eprintln!("fdb-sys: panic in Rust reader (suppressed at FFI boundary)");
            -1
        }
    }
}

// =============================================================================
// Helper functions for creating callbacks
// =============================================================================

/// Create a flush callback from a closure.
pub fn make_flush_callback<F>(f: F) -> Box<FlushCallbackBox>
where
    F: Fn() + Send + 'static,
{
    struct ClosureCallback<F>(F);
    impl<F: Fn() + Send> FlushCallback for ClosureCallback<F> {
        fn on_flush(&self) {
            (self.0)();
        }
    }
    Box::new(FlushCallbackBox(Box::new(ClosureCallback(f))))
}

/// Create an archive callback from a closure.
pub fn make_archive_callback<F>(f: F) -> Box<ArchiveCallbackBox>
where
    F: Fn(ArchiveCallbackData) + Send + 'static,
{
    struct ClosureCallback<F>(F);
    impl<F: Fn(ArchiveCallbackData) + Send> ArchiveCallback for ClosureCallback<F> {
        fn on_archive(&self, data: ArchiveCallbackData) {
            (self.0)(data);
        }
    }
    Box::new(ArchiveCallbackBox(Box::new(ClosureCallback(f))))
}

/// Wrap a Rust [`std::io::Read`] source in a [`ReaderBox`].
///
/// Used by the high-level `Fdb::archive_reader` to bridge any Rust
/// `Read` into the C++ `eckit::DataHandle` consumed by
/// `fdb5::FDB::archive`.
pub fn make_reader_box<R>(reader: R) -> Box<ReaderBox>
where
    R: std::io::Read + Send + 'static,
{
    Box::new(ReaderBox(Box::new(reader)))
}

pub use ffi::*;

// Re-export cxx types needed by downstream crates
pub use cxx::{Exception, UniquePtr};

#[cfg(test)]
mod tests {
    use super::ffi;

    #[test]
    fn test_eckit_exception_caught_by_trycatch() {
        let result = ffi::test_throw_eckit_exception();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // Generic eckit::Exception gets ECKIT: prefix
        assert!(
            err.what().starts_with("ECKIT: "),
            "Expected ECKIT: prefix, got: {}",
            err.what()
        );
        assert!(
            err.what().contains("test eckit exception"),
            "Expected eckit exception message, got: {}",
            err.what()
        );
    }

    #[test]
    fn test_eckit_serious_bug_caught_by_trycatch() {
        let result = ffi::test_throw_eckit_serious_bug();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // SeriousBug gets specific prefix
        assert!(
            err.what().starts_with("ECKIT_SERIOUS_BUG: "),
            "Expected ECKIT_SERIOUS_BUG: prefix, got: {}",
            err.what()
        );
        assert!(
            err.what().contains("test serious bug"),
            "Expected serious bug message, got: {}",
            err.what()
        );
    }

    #[test]
    fn test_eckit_user_error_caught_by_trycatch() {
        let result = ffi::test_throw_eckit_user_error();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // UserError gets specific prefix
        assert!(
            err.what().starts_with("ECKIT_USER_ERROR: "),
            "Expected ECKIT_USER_ERROR: prefix, got: {}",
            err.what()
        );
        assert!(
            err.what().contains("test user error"),
            "Expected user error message, got: {}",
            err.what()
        );
    }

    #[test]
    fn test_std_exception_caught_by_trycatch() {
        let result = ffi::test_throw_std_exception();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // std::exception should NOT have any ECKIT prefix
        assert!(
            !err.what().starts_with("ECKIT"),
            "std::exception should not have ECKIT prefix, got: {}",
            err.what()
        );
        assert!(
            err.what().contains("test std exception"),
            "Expected std exception message, got: {}",
            err.what()
        );
    }

    #[test]
    fn test_non_std_exception_caught_by_trycatch() {
        let result = ffi::test_throw_int();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // Non-std exceptions get a generic message
        assert!(
            err.what().contains("non-std::exception"),
            "Expected non-std::exception message, got: {}",
            err.what()
        );
    }
}
