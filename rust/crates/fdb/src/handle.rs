//! FDB handle wrapper.

use std::collections::HashMap;
use std::sync::Once;

use fdb_sys::UniquePtr;
use fdb_sys::{ControlAction, ControlIdentifier};
use parking_lot::Mutex;

use crate::datareader::DataReader;
use crate::error::Result;
use crate::iterator::{
    ControlIterator, DumpIterator, ListIterator, MoveIterator, PurgeIterator, StatsIterator,
    StatusIterator, WipeIterator,
};
use crate::key::Key;
use crate::options::{DumpOptions, ListOptions, PurgeOptions, WipeOptions};
use crate::request::Request;

static INIT: Once = Once::new();

/// Initialize the FDB library.
/// Called automatically when creating any FDB handle.
fn initialize() {
    INIT.call_once(fdb_sys::fdb_init);
}

/// Convert a path to a `&str`, returning a typed `UserError` if it isn't
/// valid UTF-8 (which the cxx bridge can't accept).
fn path_to_str(path: &std::path::Path) -> Result<&str> {
    path.to_str().ok_or_else(|| {
        crate::Error::UserError(format!(
            "FDB config path is not valid UTF-8: {}",
            path.display()
        ))
    })
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
/// use fdb::{Fdb, Request};
/// use std::sync::Arc;
/// use std::thread;
///
/// let fdb = Arc::new(Fdb::open_default().expect("failed to create FDB handle"));
///
/// let handles: Vec<_> = (0..4).map(|_| {
///     let fdb = Arc::clone(&fdb);
///     thread::spawn(move || {
///         let request = Request::new().with("class", "od");
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

/// One of the shapes the main FDB config can take when opening an `Fdb`.
///
/// You generally don't construct this directly — [`Fdb::open`] accepts any
/// `Option<impl Into<FdbConfig>>`, and the standard `From` impls let you
/// pass `&str`/`&String` (interpreted as inline YAML) or `&Path`/`&PathBuf`
/// (interpreted as a path to a config file on disk) directly.
///
/// Mirrors the shape of pyfdb's `config: str | Path | None` argument.
///
/// Note that this enum is for the *main* config only. The user config
/// (second argument of [`Fdb::open`]) takes only YAML strings — upstream
/// `fdb5::Config` does not have a path-based user-config entry point.
#[derive(Debug, Clone)]
pub enum FdbConfig<'a> {
    /// Inline YAML. Goes through `eckit::YAMLConfiguration` on the C++ side.
    Yaml(&'a str),
    /// Path to a YAML/JSON config file. Goes through `fdb5::Config::make`,
    /// which also expands `~fdb`/`fdb_home` references and resolves
    /// transitive sub-configurations.
    Path(&'a std::path::Path),
}

impl<'a> From<&'a str> for FdbConfig<'a> {
    fn from(s: &'a str) -> Self {
        FdbConfig::Yaml(s)
    }
}

impl<'a> From<&'a String> for FdbConfig<'a> {
    fn from(s: &'a String) -> Self {
        FdbConfig::Yaml(s.as_str())
    }
}

impl<'a> From<&'a std::path::Path> for FdbConfig<'a> {
    fn from(p: &'a std::path::Path) -> Self {
        FdbConfig::Path(p)
    }
}

impl<'a> From<&'a std::path::PathBuf> for FdbConfig<'a> {
    fn from(p: &'a std::path::PathBuf) -> Self {
        FdbConfig::Path(p.as_path())
    }
}

impl Fdb {
    /// Open an FDB.
    ///
    /// `config` is the main FDB configuration. It accepts anything
    /// convertible to [`FdbConfig`]: a `&str`/`&String` (inline YAML), a
    /// `&Path`/`&PathBuf` (config file on disk), or `None` to use the
    /// upstream's environment-driven defaults (`FDB_HOME` /
    /// `FDB_CONFIG_FILE` / `~/.fdb`).
    ///
    /// `user_config` is an optional per-instance YAML overlay (e.g.
    /// `useSubToc: true`, `preloadTocBTree: false`). It accepts only a
    /// YAML string because upstream `fdb5::Config` itself only takes the
    /// user config as an in-memory `eckit::Configuration`, never as a
    /// path. A user config without a main config is rejected — there's
    /// nothing for the overlay to apply to.
    ///
    /// Mirrors pyfdb's `FDB(config, user_config)` constructor shape, with
    /// two improvements: (1) `(None, Some(user_config))` is rejected
    /// instead of silently dropping the user config like pyfdb does, and
    /// (2) the unsupported `Path` user-config shape is forbidden at the
    /// type level rather than at runtime.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use fdb::Fdb;
    /// use std::path::Path;
    ///
    /// // Inline YAML, no user config:
    /// let fdb = Fdb::open(Some("type: local\nschema: /tmp/schema\nspaces: []"), None)?;
    ///
    /// // Config file on disk:
    /// let fdb = Fdb::open(Some(Path::new("/etc/fdb/config.yaml")), None)?;
    ///
    /// // Path config + inline user config to enable sub-tocs:
    /// let fdb = Fdb::open(
    ///     Some(Path::new("/etc/fdb/config.yaml")),
    ///     Some("useSubToc: true"),
    /// )?;
    /// # Ok::<(), fdb::Error>(())
    /// ```
    ///
    /// For the "use defaults from environment" case where neither argument
    /// is supplied, prefer [`Self::open_default`] — it avoids Rust's
    /// type-inference annoyance with `Fdb::open(None, None)`.
    ///
    /// # Errors
    ///
    /// - `UserError` if a non-UTF-8 path is supplied (the cxx bridge can't
    ///   accept it).
    /// - `UserError` if `user_config` is supplied without a `config`.
    /// - Whatever `eckit`/`fdb5` raises if the configuration can't be
    ///   parsed or the FDB instance can't be constructed.
    pub fn open<'a, C>(config: Option<C>, user_config: Option<&str>) -> Result<Self>
    where
        C: Into<FdbConfig<'a>>,
    {
        initialize();
        let config = config.map(Into::into);

        // Map (config, user_config) to one of the existing cxx-bridge
        // entry points. The arms below cover exactly the combinations
        // upstream `fdb5::Config` supports — there are no invented arms.
        let handle = match (config, user_config) {
            (None, None) => fdb_sys::new_fdb()?,
            (Some(FdbConfig::Yaml(yaml)), None) => fdb_sys::new_fdb_from_yaml(yaml)?,
            (Some(FdbConfig::Path(path)), None) => {
                let path_str = path_to_str(path)?;
                fdb_sys::new_fdb_from_path(path_str)?
            }
            (Some(FdbConfig::Yaml(yaml)), Some(user)) => {
                fdb_sys::new_fdb_from_yaml_with_user_config(yaml, user)?
            }
            (Some(FdbConfig::Path(path)), Some(user)) => {
                let path_str = path_to_str(path)?;
                fdb_sys::new_fdb_from_path_with_user_config(path_str, user)?
            }
            // pyfdb silently drops `user_config` here. We don't — there's
            // no upstream entry point that says "env-default config plus
            // this user overlay", and silently dropping is a footgun.
            (None, Some(_)) => {
                return Err(crate::Error::UserError(
                    "Fdb::open: user_config requires a main config".to_string(),
                ));
            }
        };

        Ok(Self {
            handle: Mutex::new(HandleInner(handle)),
        })
    }

    /// Open an FDB using the upstream's default configuration discovery
    /// (`FDB_HOME` / `FDB_CONFIG_FILE` / `~/.fdb`). Equivalent to
    /// `Fdb::open(None::<&str>, None)`, but avoids the type-inference
    /// annoyance with the bare `Fdb::open(None, None)` form.
    pub fn open_default() -> Result<Self> {
        Self::open(None::<&str>, None)
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
        self.with_handle(|h| fdb_sys::archive(h, key.to_cxx(), data))?;
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
    pub fn list(&self, request: &Request, options: ListOptions) -> Result<ListIterator> {
        let ListOptions { depth, deduplicate } = options;
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
        self.with_handle(|h| fdb_sys::archive_reader(h, boxed))?;
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
    pub fn dump(&self, request: &Request, options: DumpOptions) -> Result<DumpIterator> {
        let DumpOptions { simple } = options;
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
    /// * `options` - Wipe flags (see [`WipeOptions`]). Defaults to a dry
    ///   run — pass `WipeOptions { doit: true, ..Default::default() }` to
    ///   actually delete.
    ///
    /// # Errors
    ///
    /// Returns an error if the wipe fails.
    pub fn wipe(&self, request: &Request, options: WipeOptions) -> Result<WipeIterator> {
        let WipeOptions {
            doit,
            porcelain,
            unsafe_wipe_all,
        } = options;
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
    /// * `options` - Purge flags (see [`PurgeOptions`]). Defaults to a dry
    ///   run — pass `PurgeOptions { doit: true, ..Default::default() }` to
    ///   actually delete.
    ///
    /// # Errors
    ///
    /// Returns an error if the purge fails.
    pub fn purge(&self, request: &Request, options: PurgeOptions) -> Result<PurgeIterator> {
        let PurgeOptions { doit, porcelain } = options;
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
    /// * `identifiers` - The feature identifiers to control (e.g.
    ///   `ControlIdentifier::Retrieve`, `ControlIdentifier::Archive`)
    ///
    /// # Errors
    ///
    /// Returns an error if the control operation fails.
    pub fn control(
        &self,
        request: &Request,
        action: ControlAction,
        identifiers: &[ControlIdentifier],
    ) -> Result<ControlIterator> {
        let it = self.with_handle(|h| {
            fdb_sys::control(h, &request.to_request_string(), action, identifiers)
        })?;
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

/// Re-export callback data type.
pub use fdb_sys::ArchiveCallbackData;
