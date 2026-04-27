//! Options structs for FDB operations that take multiple optional flags.
//!
//! Rust has no language-level default arguments, so methods like
//! [`Fdb::wipe`](crate::Fdb::wipe) historically took every flag as a
//! positional `bool`, forcing every caller to write
//! `fdb.wipe(&req, false, false, false)` for the safe defaults. That made
//! the safe call site syntactically identical to the dangerous one
//! (`fdb.wipe(&req, true, false, true)`), and forced unrelated changes
//! every time upstream added a flag.
//!
//! These options structs follow the standard Rust idiom: each is
//! `Default`-derived with safe values, and callers spread the rest with
//! `..Default::default()`:
//!
//! ```no_run
//! use fdb::{Fdb, WipeOptions};
//!
//! # fn main() -> fdb::Result<()> {
//! eckit::init();
//! let fdb = Fdb::open_default()?;
//! let request = metkit::MarsRequestBuilder::new("retrieve")
//!     .with("class", "od")
//!     .build();
//!
//! // Dry run with safe defaults — clearly the safe case.
//! for entry in fdb.wipe(&request, WipeOptions::default())? { let _ = entry?; }
//!
//! // Real wipe — the destructive flag is named, not positional.
//! for entry in fdb.wipe(&request, WipeOptions { doit: true, ..Default::default() })? {
//!     let _ = entry?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Defaults match upstream FDB tools and pyfdb:
//! - `WipeOptions`, `PurgeOptions`: every flag `false` (no destructive
//!   action without an explicit opt-in).
//! - `ListOptions`: `depth = 3`, `deduplicate = true` — full traversal,
//!   masked entries hidden, matching `fdb-list`'s defaults.
//! - `DumpOptions`: `simple = false` — verbose dump by default, matching
//!   `fdb-dump`.

/// Per-instance FDB user configuration.
///
/// Overlays the main FDB config with per-instance tuning parameters.
/// Pass to [`Fdb::open`](crate::Fdb::open) as the `user_config` argument.
///
/// # Example
///
/// ```no_run
/// use fdb::{Fdb, UserConfig};
///
/// let cfg: eckit::Config = "type: local\nspaces: []".parse()?;
/// let fdb = Fdb::open(
///     Some(&cfg),
///     Some(UserConfig { use_sub_toc: true, ..Default::default() }),
/// )?;
/// # Ok::<(), fdb::Error>(())
/// ```
#[derive(Debug, Clone, Copy)]
pub struct UserConfig {
    /// Enable sub-TOC files for improved write performance.
    /// Default: `false`.
    pub use_sub_toc: bool,
    /// Preload `BTree` index into memory on open for faster lookups.
    /// Default: `true`.
    pub preload_toc_btree: bool,
    /// Maximum read size limit for remote FDB (bytes).
    /// Default: 1 GiB.
    pub read_limit: i64,
}

impl Default for UserConfig {
    fn default() -> Self {
        Self {
            use_sub_toc: false,
            preload_toc_btree: true,
            read_limit: 1024 * 1024 * 1024, // 1 GiB
        }
    }
}

impl From<UserConfig> for eckit::Config {
    fn from(cfg: UserConfig) -> Self {
        let mut config = Self::new();
        config.set("useSubToc", cfg.use_sub_toc);
        config.set("preloadTocBTree", cfg.preload_toc_btree);
        config.set("limits.read", cfg.read_limit);
        config
    }
}

/// Options for [`Fdb::list`](crate::Fdb::list).
///
/// Defaults match `fdb-list`'s defaults: full-depth traversal, masked
/// entries hidden.
#[derive(Debug, Clone, Copy)]
pub struct ListOptions {
    /// Index level to traverse: 1 = database, 2 = +index, 3 = +datum.
    /// Default: 3.
    pub depth: i32,
    /// Hide masked / duplicate entries (the default `fdb-list` behaviour).
    /// Set to `false` to see all entries including masked ones.
    /// Default: `true`.
    pub deduplicate: bool,
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            depth: 3,
            deduplicate: true,
        }
    }
}

/// Options for [`Fdb::wipe`](crate::Fdb::wipe).
///
/// Every flag defaults to `false` — `wipe` is a dry run unless the caller
/// explicitly opts in.
#[derive(Debug, Clone, Copy, Default)]
pub struct WipeOptions {
    /// Actually perform the wipe. With `false` (the default), the call is
    /// a dry run that lists what *would* be deleted.
    pub doit: bool,
    /// Restrict the output to the wiped files (matches `fdb-wipe
    /// --porcelain`).
    pub porcelain: bool,
    /// Disable safety checks and force a wipe even when the request would
    /// otherwise be rejected. **Dangerous.**
    pub unsafe_wipe_all: bool,
}

/// Options for [`Fdb::purge`](crate::Fdb::purge).
///
/// Every flag defaults to `false` — `purge` is a dry run unless the
/// caller explicitly opts in.
#[derive(Debug, Clone, Copy, Default)]
pub struct PurgeOptions {
    /// Actually perform the purge. With `false` (the default), the call
    /// is a dry run.
    pub doit: bool,
    /// Restrict the output to the purged files.
    pub porcelain: bool,
}

/// Options for [`Fdb::dump`](crate::Fdb::dump).
#[derive(Debug, Clone, Copy, Default)]
pub struct DumpOptions {
    /// Use the simple (one-line-per-field) output format. Default
    /// `false` produces the verbose multi-line format that matches
    /// upstream `fdb-dump`.
    pub simple: bool,
}
