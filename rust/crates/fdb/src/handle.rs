//! FDB handle wrapper.

use std::collections::HashMap;
use std::sync::Once;

use fdb_sys::UniquePtr;
use fdb_sys::{ControlAction, ControlIdentifier};
use parking_lot::Mutex;

use crate::error::Result;
use crate::iterator::{
    ControlIterator, DumpIterator, ListIterator, PurgeIterator, StatsIterator, StatusIterator,
    WipeIterator,
};
use crate::key::Key;
use crate::options::{DumpOptions, ListOptions, PurgeOptions, WipeOptions};
use eckit::DataHandle;

static INIT: Once = Once::new();

/// Initialize the FDB library.
/// Called automatically when creating any FDB handle.
fn initialize() {
    INIT.call_once(fdb_sys::Library::initialise);
}

// Private wrapper to make UniquePtr Send-safe for use with Mutex
struct HandleInner(UniquePtr<fdb_sys::FdbHandle>);

// SAFETY: HandleInner is only accessed through Mutex which provides synchronization.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for HandleInner {}

/// A handle to a single FDB instance (wraps `fdb5::FDB`).
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
/// use fdb::Fdb;
/// use std::sync::Arc;
/// use std::thread;
///
/// eckit::init();
/// let fdb = Arc::new(Fdb::open_default().expect("failed to create FDB handle"));
///
/// let handles: Vec<_> = (0..4).map(|_| {
///     let fdb = Arc::clone(&fdb);
///     thread::spawn(move || {
///         let request = metkit::MarsRequestBuilder::new("list")
///             .with("class", "od")
///             .build();
///         let _ = fdb.list(&request, fdb::ListOptions::default());
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
    /// Open an FDB.
    ///
    /// Matches C++ `fdb5::FDB(fdb5::Config)` / `fdb5::FDB(fdb5::Config(config, user_config))`.
    ///
    /// - `None, None` — use environment defaults (`FDB_HOME` / `FDB_CONFIG_FILE` / `~/.fdb`)
    /// - `Some(config), None` — use the given config
    /// - `Some(config), Some(user_config)` — config + per-instance overlay
    ///
    /// Build the `eckit::Config` however you want: `Config::from_path()`,
    /// `"yaml".parse()`, or `Config::new()` + `.set()`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use fdb::{Fdb, UserConfig};
    ///
    /// // Default config from environment:
    /// let fdb = Fdb::open(None, None)?;
    ///
    /// // From a YAML file:
    /// let cfg = eckit::Config::from_path("/etc/fdb/config.yaml")?;
    /// let fdb = Fdb::open(Some(&cfg), None)?;
    ///
    /// // Inline YAML:
    /// let cfg: eckit::Config = "type: local\nspaces: []".parse()?;
    /// let fdb = Fdb::open(Some(&cfg), None)?;
    ///
    /// // With user config:
    /// let cfg = eckit::Config::from_path("/etc/fdb/config.yaml")?;
    /// let fdb = Fdb::open(
    ///     Some(&cfg),
    ///     Some(UserConfig { use_sub_toc: true, ..Default::default() }),
    /// )?;
    /// # Ok::<(), fdb::Error>(())
    /// ```
    pub fn open(
        config: Option<&eckit::Config>,
        user_config: Option<crate::UserConfig>,
    ) -> Result<Self> {
        initialize();

        let user_eckit = user_config.map(eckit::Config::from);

        let handle = match (config, user_eckit.as_ref()) {
            (None, None) => fdb_sys::FdbHandle::create()?,
            (Some(cfg), None) => fdb_sys::FdbHandle::from_config(cfg.as_sys())?,
            (Some(cfg), Some(user)) => {
                fdb_sys::FdbHandle::from_config_with_user(cfg.as_sys(), user.as_sys())?
            }
            (None, Some(_)) => {
                return Err(crate::Error::Eckit(eckit::Error::UserError(
                    "Fdb::open: user_config requires a main config".to_string(),
                )));
            }
        };

        Ok(Self {
            handle: Mutex::new(HandleInner(handle)),
        })
    }

    /// Open an FDB using environment defaults.
    ///
    /// Equivalent to `Fdb::open(None, None)`.
    pub fn open_default() -> Result<Self> {
        Self::open(None, None)
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
        self.with_handle(|h| h.archive(key.to_cxx(), data))?;
        Ok(())
    }

    /// List fields matching a request.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which fields to list
    /// * `options` - Traversal depth and deduplication flag (see
    ///   [`ListOptions`]). Defaults match `fdb-list`: full-depth traversal,
    ///   masked entries hidden.
    ///
    /// # Errors
    ///
    /// Returns an error if listing fails.
    pub fn list(
        &self,
        request: &metkit::MarsRequest,
        options: ListOptions,
    ) -> Result<ListIterator> {
        let ListOptions { depth, deduplicate } = options;
        let it = self.with_handle(|h| h.list(request.as_sys(), deduplicate, depth))?;
        Ok(ListIterator::new(it))
    }

    /// Retrieve data from FDB using a `MarsRequest`.
    ///
    /// Returns an `eckit::DataHandle` opened for reading.
    ///
    /// # Errors
    ///
    /// Returns an error if retrieval fails.
    pub fn retrieve(&self, request: &metkit::MarsRequest) -> Result<DataHandle> {
        let handle = self.with_handle(|h| h.retrieve(request.as_sys()))?;
        Ok(DataHandle::from_raw(handle))
    }

    /// Read data from a single URI location.
    ///
    /// More efficient than `retrieve()` when you already have
    /// the field location from a previous `list()` operation.
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub fn read_uri(&self, uri: &str) -> Result<DataHandle> {
        let handle = self.with_handle(|h| h.read_uri(uri))?;
        Ok(DataHandle::from_raw(handle))
    }

    /// Read data from multiple URI locations.
    ///
    /// More efficient than `retrieve()` when you already have
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
    pub fn read_uris(&self, uris: &[String], in_storage_order: bool) -> Result<DataHandle> {
        let uris_vec: Vec<String> = uris.to_vec();
        let handle = self.with_handle(|h| h.read_uris(&uris_vec, in_storage_order))?;
        Ok(DataHandle::from_raw(handle))
    }

    /// Read data directly from a list iterator (most efficient).
    ///
    /// Consumes the iterator and reads all matched fields.
    /// More efficient than `read_uris()` as it avoids URI string conversion.
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub fn read_from_list(
        &self,
        mut list: ListIterator,
        in_storage_order: bool,
    ) -> Result<DataHandle> {
        let handle =
            self.with_handle(|h| h.read_list_iterator(list.inner_mut(), in_storage_order))?;
        Ok(DataHandle::from_raw(handle))
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
        self.with_handle(|h| h.archive_raw(data))?;
        Ok(())
    }

    /// Archive raw GRIB data streamed from an arbitrary [`std::io::Read`]
    /// source.
    ///
    /// The C++ side wraps the reader in an `eckit::DataHandle` and hands
    /// it to `fdb5::FDB::archive(eckit::DataHandle&)`, which extracts the
    /// key from each GRIB message as it streams. This is the streaming
    /// equivalent of [`Self::archive_raw`] — useful for archiving from a
    /// file, network socket, or any other `Read` source without
    /// buffering the entire payload in memory first.
    ///
    /// # Errors
    ///
    /// Returns an error if archiving fails (including I/O errors raised
    /// by the supplied reader, surfaced from the C++ side as an
    /// `eckit::ReadError`).
    pub fn archive_reader<R>(&self, reader: R) -> Result<()>
    where
        R: std::io::Read + Send + 'static,
    {
        let boxed = fdb_sys::make_reader_box(reader);
        self.with_handle(|h| h.archive_reader(boxed))?;
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
    pub fn axes(
        &self,
        request: &metkit::MarsRequest,
        depth: i32,
    ) -> Result<HashMap<String, Vec<String>>> {
        let axes = self.with_handle(|h| h.axes(request.as_sys(), depth))?;
        Ok(axes.into_iter().map(|a| (a.key, a.values)).collect())
    }

    /// Dump database structure.
    ///
    /// # Arguments
    ///
    /// * `request` - The request to filter which databases to dump
    /// * `options` - Output format flags (see [`DumpOptions`]). Defaults
    ///   to the verbose multi-line format that matches `fdb-dump`.
    ///
    /// # Errors
    ///
    /// Returns an error if the dump fails.
    pub fn dump(
        &self,
        request: &metkit::MarsRequest,
        options: DumpOptions,
    ) -> Result<DumpIterator> {
        let DumpOptions { simple } = options;
        let it = self.with_handle(|h| h.dump(request.as_sys(), simple))?;
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
    pub fn status(&self, request: &metkit::MarsRequest) -> Result<StatusIterator> {
        let it = self.with_handle(|h| h.status(request.as_sys()))?;
        Ok(StatusIterator::new(it))
    }

    /// Wipe (delete) data matching a request.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which data to wipe
    /// * `options` - Wipe flags (see [`WipeOptions`]). Defaults to a dry
    ///   run — pass `WipeOptions { doit: true, ..Default::default() }` to
    ///   actually delete.
    ///
    /// # Errors
    ///
    /// Returns an error if the wipe fails.
    pub fn wipe(
        &self,
        request: &metkit::MarsRequest,
        options: WipeOptions,
    ) -> Result<WipeIterator> {
        let WipeOptions {
            doit,
            porcelain,
            unsafe_wipe_all,
        } = options;
        let it =
            self.with_handle(|h| h.wipe(request.as_sys(), doit, porcelain, unsafe_wipe_all))?;
        Ok(WipeIterator::new(it))
    }

    /// Purge duplicate data.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which data to purge
    /// * `options` - Purge flags (see [`PurgeOptions`]). Defaults to a dry
    ///   run — pass `PurgeOptions { doit: true, ..Default::default() }` to
    ///   actually delete.
    ///
    /// # Errors
    ///
    /// Returns an error if the purge fails.
    pub fn purge(
        &self,
        request: &metkit::MarsRequest,
        options: PurgeOptions,
    ) -> Result<PurgeIterator> {
        let PurgeOptions { doit, porcelain } = options;
        let it = self.with_handle(|h| h.purge(request.as_sys(), doit, porcelain))?;
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
    pub fn stats_iter(&self, request: &metkit::MarsRequest) -> Result<StatsIterator> {
        let it = self.with_handle(|h| h.stats_iterator(request.as_sys()))?;
        Ok(StatsIterator::new(it))
    }

    /// Control database features.
    ///
    /// # Arguments
    ///
    /// * `request` - The request specifying which databases to control
    /// * `action` - The action to perform
    /// * `identifiers` - The feature identifiers to control (e.g.
    ///   `ControlIdentifier::Retrieve`, `ControlIdentifier::Archive`)
    ///
    /// # Errors
    ///
    /// Returns an error if the control operation fails.
    pub fn control(
        &self,
        request: &metkit::MarsRequest,
        action: ControlAction,
        identifiers: &[ControlIdentifier],
    ) -> Result<ControlIterator> {
        let it = self.with_handle(|h| h.control(request.as_sys(), action, identifiers))?;
        Ok(ControlIterator::new(it))
    }

    /// Check if a control identifier is enabled.
    ///
    /// # Arguments
    ///
    /// * `identifier` - The identifier to check (e.g.
    ///   `ControlIdentifier::Retrieve`, `ControlIdentifier::Archive`)
    #[must_use]
    pub fn enabled(&self, identifier: ControlIdentifier) -> bool {
        self.with_handle_ref(|h| h.enabled(identifier))
    }

    /// Register a callback to be invoked on flush.
    pub fn on_flush<F>(&self, callback: F)
    where
        F: Fn() + Send + 'static,
    {
        self.with_handle(|h| {
            h.register_flush_callback(fdb_sys::make_flush_callback(callback));
        });
    }

    /// Register a callback to be invoked for each archived field.
    pub fn on_archive<F>(&self, callback: F)
    where
        F: Fn(ArchiveCallbackData) + Send + 'static,
    {
        self.with_handle(|h| {
            h.register_archive_callback(fdb_sys::make_archive_callback(callback));
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

/// Re-export callback data type.
pub use fdb_sys::ArchiveCallbackData;

/// Wrapper for `fdb5::MessageArchiver`.
///
/// This is the same class used by mars-client-cpp's `FDBBase::archive`. Use
/// this when you want a literal port of the C++ archiving call path (filters,
/// modifiers, etc.) rather than going through `Fdb::archive_raw` /
/// `Fdb::archive_reader` which use `fdb5::FDB::archive`.
pub struct MessageArchiver {
    inner: Mutex<UniquePtr<fdb_sys::MessageArchiverWrapper>>,
}

impl MessageArchiver {
    /// Construct an archiver. `key` is the modifier key applied to every
    /// message (use `Key::new()` for none, matching C++ `FDBBase`).
    /// `complete_transfers` and `verbose` map directly to the
    /// `fdb5::MessageArchiver` ctor flags (mars-client-cpp uses `false`).
    pub fn new(
        key: &Key,
        complete_transfers: bool,
        verbose: bool,
        config: &eckit::Config,
    ) -> Result<Self> {
        initialize();
        let inner = fdb_sys::MessageArchiverWrapper::create(
            key.to_cxx(),
            complete_transfers,
            verbose,
            config.as_sys(),
        )?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    /// `fdb5::MessageArchiver::archive(eckit::DataHandle&)` — returns total
    /// bytes archived.
    pub fn archive(&self, source: &mut eckit::DataHandle<eckit::Reading>) -> Result<i64> {
        let mut guard = self.inner.lock();
        let bytes = guard.pin_mut().archive(source.inner_mut()?)?;
        drop(guard);
        Ok(bytes)
    }

    /// `fdb5::MessageArchiver::flush()`.
    pub fn flush(&self) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.pin_mut().flush()?;
        drop(guard);
        Ok(())
    }
}

// SAFETY: cxx UniquePtr is `!Send` by default; the Mutex serialises every
// access to the underlying C++ object so moving the wrapper between threads
// is safe.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for MessageArchiver {}
