//! FDB handle wrapper.

use std::collections::HashMap;
use std::sync::Once;

use fdb_sys::ControlAction;
use fdb_sys::UniquePtr;
use parking_lot::Mutex;

use crate::datareader::DataReader;
use crate::error::Result;
use crate::iterator::{
    AxesIterator, ControlIterator, DumpIterator, ListIterator, MoveIterator, PurgeIterator,
    StatsIterator, StatusIterator, WipeIterator,
};
use crate::key::Key;
use crate::request::Request;

static INIT: Once = Once::new();

/// Initialize the FDB library.
/// Called automatically when creating any FDB handle.
fn initialize() {
    INIT.call_once(fdb_sys::fdb_init);
}

// Private wrapper to make UniquePtr Send-safe for use with Mutex
struct HandleInner(UniquePtr<fdb_sys::FdbHandle>);

// SAFETY: HandleInner is only accessed through Mutex which provides synchronization.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for HandleInner {}

/// A handle to the FDB library.
///
/// This is the main entry point for FDB operations.
///
/// # Thread Safety
///
/// `Fdb` implements `Send + Sync` and can be shared across threads via `Arc<Fdb>`.
/// All methods use internal locking to ensure thread-safe access.
///
/// # Example
///
/// ```no_run
/// use fdb::{Fdb, Request};
/// use std::sync::Arc;
/// use std::thread;
///
/// let fdb = Arc::new(Fdb::new().expect("failed to create FDB handle"));
///
/// let handles: Vec<_> = (0..4).map(|_| {
///     let fdb = Arc::clone(&fdb);
///     thread::spawn(move || {
///         let request = Request::new().with("class", "od");
///         let _ = fdb.list(&request, 1, false);
///     })
/// }).collect();
///
/// for h in handles {
///     h.join().unwrap();
/// }
/// ```
pub struct Fdb {
    handle: Mutex<HandleInner>,
}

impl Fdb {
    /// Create a new FDB handle with default configuration.
    pub fn new() -> Result<Self> {
        initialize();
        let handle = fdb_sys::new_fdb()?;
        Ok(Self {
            handle: Mutex::new(HandleInner(handle)),
        })
    }

    /// Create a new FDB handle from a YAML configuration.
    pub fn from_yaml(config: &str) -> Result<Self> {
        initialize();
        let handle = fdb_sys::new_fdb_from_yaml(config)?;
        Ok(Self {
            handle: Mutex::new(HandleInner(handle)),
        })
    }

    /// Create a new FDB handle from a YAML configuration plus a per-instance
    /// "user config" (also YAML).
    ///
    /// The user config corresponds to the second argument of
    /// `fdb5::Config::Config(...)` and carries runtime overrides such as
    /// `useSubToc: true` or `preloadTocBTree: true` that are not part of the
    /// shared FDB configuration file.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fdb::Fdb;
    ///
    /// let config = "type: local\nengine: toc\nschema: /tmp/schema\nspaces: []";
    /// let user_config = "useSubToc: true";
    /// let fdb = Fdb::from_yaml_with_user_config(config, user_config)?;
    /// # Ok::<(), fdb::Error>(())
    /// ```
    pub fn from_yaml_with_user_config(config: &str, user_config: &str) -> Result<Self> {
        initialize();
        let handle = fdb_sys::new_fdb_from_yaml_with_user_config(config, user_config)?;
        Ok(Self {
            handle: Mutex::new(HandleInner(handle)),
        })
    }

    #[inline]
    fn with_handle<F, R>(&self, f: F) -> R
    where
        F: FnOnce(std::pin::Pin<&mut fdb_sys::FdbHandle>) -> R,
    {
        let mut guard = self.handle.lock();
        f(guard.0.pin_mut())
    }

    #[inline]
    fn with_handle_ref<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&fdb_sys::FdbHandle) -> R,
    {
        let guard = self.handle.lock();
        f(&guard.0)
    }

    /// Get the FDB library version.
    #[must_use]
    pub fn version() -> String {
        fdb_sys::fdb_version()
    }

    /// Get the FDB git SHA1.
    #[must_use]
    pub fn git_sha1() -> String {
        fdb_sys::fdb_git_sha1()
    }

    /// Archive data to FDB.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the data
    /// * `data` - The data to archive
    ///
    /// # Errors
    ///
    /// Returns an error if archiving fails.
    pub fn archive(&self, key: &Key, data: &[u8]) -> Result<()> {
        self.with_handle(|h| fdb_sys::archive(h, &key.to_cxx(), data))?;
        Ok(())
    }

    /// List fields matching a request.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which fields to list
    /// * `depth` - Index depth to traverse (1=database, 2=index, 3=full)
    /// * `deduplicate` - Whether to exclude duplicate entries
    ///
    /// # Errors
    ///
    /// Returns an error if listing fails.
    pub fn list(&self, request: &Request, depth: i32, deduplicate: bool) -> Result<ListIterator> {
        let it = self
            .with_handle(|h| fdb_sys::list(h, &request.to_request_string(), deduplicate, depth))?;
        Ok(ListIterator::new(it))
    }

    /// Retrieve data from FDB.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which data to retrieve
    ///
    /// # Errors
    ///
    /// Returns an error if retrieval fails.
    pub fn retrieve(&self, request: &Request) -> Result<DataReader> {
        let handle = self.with_handle(|h| fdb_sys::retrieve(h, &request.to_request_string()))?;
        DataReader::new(handle)
    }

    /// Read data from a single URI location.
    ///
    /// This is more efficient than `retrieve()` when you already have
    /// the field location from a previous `list()` operation.
    ///
    /// # Arguments
    ///
    /// * `uri` - The URI to read from
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub fn read_uri(&self, uri: &str) -> Result<DataReader> {
        let handle = self.with_handle(|h| fdb_sys::read_uri(h, uri))?;
        DataReader::new(handle)
    }

    /// Read data from multiple URI locations.
    ///
    /// This is more efficient than `retrieve()` when you already have
    /// the field locations from a previous `list()` operation.
    ///
    /// # Arguments
    ///
    /// * `uris` - List of URI strings to read from
    /// * `in_storage_order` - If true, data is returned in storage order;
    ///   if false, in the order requested
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub fn read_uris(&self, uris: &[String], in_storage_order: bool) -> Result<DataReader> {
        let uris_vec: Vec<String> = uris.to_vec();
        let handle = self.with_handle(|h| fdb_sys::read_uris(h, &uris_vec, in_storage_order))?;
        DataReader::new(handle)
    }

    /// Read data directly from a list iterator (most efficient).
    ///
    /// This consumes the iterator and reads all matched fields.
    /// More efficient than `read_uris()` as it avoids URI string conversion.
    ///
    /// # Arguments
    ///
    /// * `list` - `ListIterator` to read from (consumed)
    /// * `in_storage_order` - If true, data is returned in storage order
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub fn read_from_list(
        &self,
        mut list: ListIterator,
        in_storage_order: bool,
    ) -> Result<DataReader> {
        let handle = self
            .with_handle(|h| fdb_sys::read_list_iterator(h, list.inner_mut(), in_storage_order))?;
        DataReader::new(handle)
    }

    /// Flush any pending writes to FDB.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails (e.g., disk full, permission error).
    pub fn flush(&self) -> Result<()> {
        self.with_handle(fdb_sys::FdbHandle::flush)?;
        Ok(())
    }

    /// Check if the FDB has unflushed data.
    #[must_use]
    pub fn dirty(&self) -> bool {
        self.with_handle_ref(fdb_sys::FdbHandle::dirty)
    }

    /// Get the FDB configuration ID.
    #[must_use]
    pub fn id(&self) -> String {
        self.with_handle_ref(fdb_sys::FdbHandle::id)
    }

    /// Get the FDB type name (e.g., "local", "remote").
    #[must_use]
    pub fn name(&self) -> String {
        self.with_handle_ref(fdb_sys::FdbHandle::name)
    }

    /// Get aggregate statistics for this FDB handle.
    #[must_use]
    pub fn stats(&self) -> FdbStats {
        self.with_handle_ref(|h| {
            let data = h.stats();
            FdbStats {
                num_archive: data.num_archive,
                num_location: data.num_location,
                num_flush: data.num_flush,
            }
        })
    }

    /// Archive raw GRIB data to FDB.
    ///
    /// The key is extracted from the GRIB message itself.
    ///
    /// # Arguments
    ///
    /// * `data` - The GRIB data to archive
    ///
    /// # Errors
    ///
    /// Returns an error if archiving fails.
    pub fn archive_raw(&self, data: &[u8]) -> Result<()> {
        self.with_handle(|h| fdb_sys::archive_raw(h, data))?;
        Ok(())
    }

    /// Get available axes (metadata dimensions) for a request.
    ///
    /// Returns a map of axis names to their available values.
    ///
    /// # Arguments
    ///
    /// * `request` - The request to query axes for
    /// * `depth` - Index depth to traverse (1=database, 2=index, 3=full)
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub fn axes(&self, request: &Request, depth: i32) -> Result<HashMap<String, Vec<String>>> {
        let axes = self.with_handle(|h| fdb_sys::axes(h, &request.to_request_string(), depth))?;
        Ok(axes.into_iter().map(|a| (a.key, a.values)).collect())
    }

    /// Get an axes iterator for streaming axes results.
    ///
    /// # Arguments
    ///
    /// * `request` - The request to query axes for
    /// * `depth` - Index depth to traverse (1=database, 2=index, 3=full)
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub fn axes_iter(&self, request: &Request, depth: i32) -> Result<AxesIterator> {
        let it =
            self.with_handle(|h| fdb_sys::axes_iterator(h, &request.to_request_string(), depth))?;
        Ok(AxesIterator::new(it))
    }

    /// Dump database structure.
    ///
    /// # Arguments
    ///
    /// * `request` - The request to filter which databases to dump
    /// * `simple` - Whether to use simple output format
    ///
    /// # Errors
    ///
    /// Returns an error if the dump fails.
    pub fn dump(&self, request: &Request, simple: bool) -> Result<DumpIterator> {
        let it = self.with_handle(|h| fdb_sys::dump(h, &request.to_request_string(), simple))?;
        Ok(DumpIterator::new(it))
    }

    /// Get database status.
    ///
    /// # Arguments
    ///
    /// * `request` - The request to filter which databases to query
    ///
    /// # Errors
    ///
    /// Returns an error if the status query fails.
    pub fn status(&self, request: &Request) -> Result<StatusIterator> {
        let it = self.with_handle(|h| fdb_sys::status(h, &request.to_request_string()))?;
        Ok(StatusIterator::new(it))
    }

    /// Wipe (delete) data matching a request.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which data to wipe
    /// * `doit` - If true, actually perform the wipe; if false, dry run
    /// * `porcelain` - If true, use machine-readable output format
    /// * `unsafe_wipe_all` - If true, allow wiping all data (dangerous)
    ///
    /// # Errors
    ///
    /// Returns an error if the wipe fails.
    pub fn wipe(
        &self,
        request: &Request,
        doit: bool,
        porcelain: bool,
        unsafe_wipe_all: bool,
    ) -> Result<WipeIterator> {
        let it = self.with_handle(|h| {
            fdb_sys::wipe(
                h,
                &request.to_request_string(),
                doit,
                porcelain,
                unsafe_wipe_all,
            )
        })?;
        Ok(WipeIterator::new(it))
    }

    /// Purge duplicate data.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which data to purge
    /// * `doit` - If true, actually perform the purge; if false, dry run
    /// * `porcelain` - If true, use machine-readable output format
    ///
    /// # Errors
    ///
    /// Returns an error if the purge fails.
    pub fn purge(&self, request: &Request, doit: bool, porcelain: bool) -> Result<PurgeIterator> {
        let it =
            self.with_handle(|h| fdb_sys::purge(h, &request.to_request_string(), doit, porcelain))?;
        Ok(PurgeIterator::new(it))
    }

    /// Get detailed statistics iterator.
    ///
    /// # Arguments
    ///
    /// * `request` - The request to filter which databases to query
    ///
    /// # Errors
    ///
    /// Returns an error if the stats query fails.
    pub fn stats_iter(&self, request: &Request) -> Result<StatsIterator> {
        let it = self.with_handle(|h| fdb_sys::stats_iterator(h, &request.to_request_string()))?;
        Ok(StatsIterator::new(it))
    }

    /// Control database features.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which databases to control
    /// * `action` - The action to perform
    /// * `identifiers` - The feature identifiers to control (e.g., "retrieve", "archive")
    ///
    /// # Errors
    ///
    /// Returns an error if the control operation fails.
    pub fn control(
        &self,
        request: &Request,
        action: ControlAction,
        identifiers: &[String],
    ) -> Result<ControlIterator> {
        let ids: Vec<String> = identifiers.to_vec();
        let it =
            self.with_handle(|h| fdb_sys::control(h, &request.to_request_string(), action, &ids))?;
        Ok(ControlIterator::new(it))
    }

    /// Move data to a new location.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which data to move
    /// * `dest` - The destination path
    ///
    /// # Errors
    ///
    /// Returns an error if the move fails.
    pub fn move_data(&self, request: &Request, dest: &str) -> Result<MoveIterator> {
        let it = self.with_handle(|h| fdb_sys::move_data(h, &request.to_request_string(), dest))?;
        Ok(MoveIterator::new(it))
    }

    /// Check if a control identifier is enabled.
    ///
    /// # Arguments
    ///
    /// * `identifier` - The identifier to check (e.g., "retrieve", "archive")
    #[must_use]
    pub fn enabled(&self, identifier: &str) -> bool {
        self.with_handle_ref(|h| h.enabled(identifier))
    }

    /// Get the FDB configuration data.
    #[must_use]
    pub fn config(&self) -> FdbConfig {
        self.with_handle_ref(|h| {
            let data = h.config();
            FdbConfig {
                schema_path: data.schema_path,
                config_path: data.config_path,
            }
        })
    }

    /// Get a string value from the FDB configuration.
    ///
    /// Returns `None` if the key doesn't exist.
    #[must_use]
    pub fn config_string(&self, key: &str) -> Option<String> {
        if self.config_has(key) {
            Some(self.with_handle_ref(|h| h.config_string(key)))
        } else {
            None
        }
    }

    /// Get an integer value from the FDB configuration.
    ///
    /// Returns `None` if the key doesn't exist.
    #[must_use]
    pub fn config_int(&self, key: &str) -> Option<i64> {
        if self.config_has(key) {
            Some(self.with_handle_ref(|h| h.config_int(key)))
        } else {
            None
        }
    }

    /// Get a boolean value from the FDB configuration.
    ///
    /// Returns `None` if the key doesn't exist.
    #[must_use]
    pub fn config_bool(&self, key: &str) -> Option<bool> {
        if self.config_has(key) {
            Some(self.with_handle_ref(|h| h.config_bool(key)))
        } else {
            None
        }
    }

    /// Check if a key exists in the FDB configuration.
    #[must_use]
    pub fn config_has(&self, key: &str) -> bool {
        self.with_handle_ref(|h| h.config_has(key))
    }

    /// Register a callback to be invoked on flush.
    pub fn on_flush<F>(&self, callback: F)
    where
        F: Fn() + Send + 'static,
    {
        self.with_handle(|h| {
            fdb_sys::register_flush_callback(h, fdb_sys::make_flush_callback(callback));
        });
    }

    /// Register a callback to be invoked for each archived field.
    pub fn on_archive<F>(&self, callback: F)
    where
        F: Fn(ArchiveCallbackData) + Send + 'static,
    {
        self.with_handle(|h| {
            fdb_sys::register_archive_callback(h, fdb_sys::make_archive_callback(callback));
        });
    }
}

// SAFETY: Fdb uses Mutex for synchronization, making it safe to send and share.
unsafe impl Send for Fdb {}
unsafe impl Sync for Fdb {}

/// Aggregate FDB statistics.
#[derive(Debug, Clone, Copy, Default)]
pub struct FdbStats {
    /// Number of archive operations.
    pub num_archive: u64,
    /// Number of location operations.
    pub num_location: u64,
    /// Number of flush operations.
    pub num_flush: u64,
}

/// FDB configuration data.
#[derive(Debug, Clone, Default)]
pub struct FdbConfig {
    /// Path to the schema file.
    pub schema_path: String,
    /// Path to the config file.
    pub config_path: String,
}

/// Re-export callback data type.
pub use fdb_sys::ArchiveCallbackData;
