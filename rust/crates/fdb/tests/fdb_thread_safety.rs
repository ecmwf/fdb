//! Thread-safety tests for `Fdb`.
//!
//! These tests verify that `Fdb` works correctly under concurrent access.
//!
//! The FDB C++ library is documented as thread-safe (fdb5/api/FDB.h:62-66):
//! "FDB and its methods are threadsafe."
//!
//! Thread-safety guarantees:
//! - `Fdb` implements `Send + Sync` (always, no feature flag required)
//! - Methods can be called from multiple threads via `Arc<Fdb>`
//! - Internal `Mutex` ensures thread-safe access to the C++ handle
//!
//! Run with `cargo test --test fdb_thread_safety`.

use std::sync::Arc;
use std::thread;

use fdb::{Fdb, Key, ListOptions, Request};

// =============================================================================
// Trait bound tests (compile-time verification)
// =============================================================================

/// Test: `Fdb` is Send (can be moved between threads)
#[test]
fn test_fdb_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Fdb>();
}

/// Test: `Fdb` is Sync (can be shared between threads via reference)
#[test]
fn test_fdb_is_sync() {
    fn assert_sync<T: Sync>() {}
    assert_sync::<Fdb>();
}

/// Test: `Key` is Send + Sync
#[test]
fn test_key_traits() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<Key>();
    assert_sync::<Key>();
}

/// Test: `Request` is Send + Sync
#[test]
fn test_request_traits() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<Request>();
    assert_sync::<Request>();
}

// =============================================================================
// Runtime tests (require FDB libraries and configuration)
// =============================================================================

/// Test: `Fdb` handle can be created
#[test]
fn test_handle_creation() {
    let fdb = Fdb::open_default();
    assert!(fdb.is_ok(), "Failed to create Fdb: {:?}", fdb.err());
}

/// Test: `Fdb` can be shared via Arc for concurrent access
#[test]
fn test_arc_sharing_readonly() {
    let fdb = Arc::new(Fdb::open_default().expect("failed to create handle"));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let fdb = Arc::clone(&fdb);
            thread::spawn(move || {
                for _ in 0..100 {
                    let _ = fdb.id();
                    let _ = fdb.name();
                    let _ = fdb.dirty();
                    let _ = fdb.stats();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}

/// Test: Concurrent read-only operations (id, name, dirty, stats)
#[test]
fn test_concurrent_readonly_methods() {
    let fdb = Arc::new(Fdb::open_default().expect("failed to create handle"));

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let fdb = Arc::clone(&fdb);
            thread::spawn(move || {
                for _ in 0..100 {
                    let _ = fdb.id();
                    let _ = fdb.name();
                    let _ = fdb.dirty();
                    let _ = fdb.stats();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}

/// Test: `Fdb` can be used for concurrent list operations
#[test]
fn test_concurrent_list_operations() {
    let fdb = Arc::new(Fdb::open_default().expect("failed to create handle"));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let fdb = Arc::clone(&fdb);
            thread::spawn(move || {
                let request = Request::new().with("class", "rd");
                for _ in 0..10 {
                    let _ = fdb.list(
                        &request,
                        ListOptions {
                            depth: 1,
                            deduplicate: false,
                        },
                    );
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}

/// Test: Concurrent axes queries
#[test]
fn test_concurrent_axes() {
    let fdb = Arc::new(Fdb::open_default().expect("failed to create handle"));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let fdb = Arc::clone(&fdb);
            thread::spawn(move || {
                let request = Request::new().with("class", "rd");
                for _ in 0..10 {
                    let _ = fdb.axes(&request, 1);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}

/// Test: Stress test with many threads
#[test]
fn test_stress_concurrent_access() {
    let fdb = Arc::new(Fdb::open_default().expect("failed to create handle"));
    let iterations = 50;
    let thread_count = 16;

    let handles: Vec<_> = (0..thread_count)
        .map(|i| {
            let fdb = Arc::clone(&fdb);
            thread::spawn(move || {
                let request = Request::new().with("class", "rd");
                for j in 0..iterations {
                    if (i + j) % 2 == 0 {
                        // Read-only operations
                        let _ = fdb.id();
                        let _ = fdb.name();
                    } else {
                        // Query operations
                        let _ = fdb.list(
                            &request,
                            ListOptions {
                                depth: 1,
                                deduplicate: false,
                            },
                        );
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked during stress test");
    }
}

/// Note: FDB has a documented caveat about `flush()`:
/// "`flush()` has global semantics - it flushes ALL archived messages from
/// ALL threads, not just the calling thread. For finer control, instantiate
/// one FDB object per thread."
///
/// This test verifies the basic behavior but users should be aware of
/// this limitation when using FDB in multi-threaded contexts with archiving.
#[test]
fn test_concurrent_errors_no_crash() {
    let fdb = Arc::new(Fdb::open_default().expect("failed to create handle"));

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let fdb = Arc::clone(&fdb);
            thread::spawn(move || {
                // Use invalid requests to trigger errors
                let value = format!("value_{i}");
                let request = Request::new().with("INVALID_KEY", &value);
                for _ in 0..20 {
                    // Ignore the error - testing that concurrent errors don't crash
                    let _ = fdb.list(
                        &request,
                        ListOptions {
                            depth: 1,
                            deduplicate: false,
                        },
                    );
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }
}

// =============================================================================
// Concurrent write tests (M15)
// =============================================================================

/// Helper to create test configuration
fn create_test_config(tmpdir: &std::path::Path) -> String {
    use std::fs;
    use std::path::PathBuf;

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let fixtures_dir = PathBuf::from(manifest_dir).join("tests/fixtures");

    // Copy schema to temp directory
    let schema_src = fixtures_dir.join("schema");
    let schema_dst = tmpdir.join("schema");
    fs::copy(&schema_src, &schema_dst).expect("failed to copy schema");

    format!(
        r"---
type: local
engine: toc
schema: {}/schema
spaces:
  - roots:
      - path: {}
",
        tmpdir.display(),
        tmpdir.display()
    )
}

/// Test: Concurrent archive operations from multiple threads.
///
/// Note: FDB documents that `flush()` has global semantics - it flushes ALL
/// archived messages from ALL threads. This test verifies that concurrent
/// archive operations don't crash, but users should be aware of this behavior.
#[test]
fn test_concurrent_archive_operations() {
    use std::fs;
    use std::path::PathBuf;

    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Arc::new(Fdb::open(Some(&config), None).expect("failed to create handle"));

    // Read GRIB data for archiving
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let grib_path = PathBuf::from(manifest_dir).join("tests/fixtures/template.grib");
    let grib_data = Arc::new(fs::read(&grib_path).expect("failed to read template.grib"));

    let thread_count = 4;
    let iterations_per_thread = 5;

    let handles: Vec<_> = (0..thread_count)
        .map(|thread_id| {
            let fdb = Arc::clone(&fdb);
            let grib_data = Arc::clone(&grib_data);
            thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    // Each thread archives with a unique step value
                    let step = format!("{}", thread_id * 100 + i);
                    let key = Key::new()
                        .with("class", "rd")
                        .with("expver", "xxxx")
                        .with("stream", "oper")
                        .with("date", "20230508")
                        .with("time", "1200")
                        .with("type", "fc")
                        .with("levtype", "sfc")
                        .with("step", &step)
                        .with("param", "151130");

                    let result = fdb.archive(&key, &grib_data);
                    assert!(
                        result.is_ok(),
                        "thread {thread_id} archive failed: {:?}",
                        result.err()
                    );
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked during concurrent archive");
    }

    // Flush all archived data
    fdb.flush().expect("flush failed");

    // Verify data was archived by listing
    let request = Request::new().with("class", "rd").with("expver", "xxxx");
    let items: Vec<_> = fdb
        .list(
            &request,
            ListOptions {
                depth: 3,
                deduplicate: false,
            },
        )
        .expect("list failed")
        .filter_map(std::result::Result::ok)
        .collect();

    let expected_count = thread_count * iterations_per_thread;
    assert_eq!(
        items.len(),
        expected_count,
        "expected {expected_count} archived items, found {}",
        items.len()
    );
}

/// Test: Mixed concurrent read and write operations.
#[test]
fn test_concurrent_read_write_mix() {
    use std::fs;
    use std::path::PathBuf;

    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Arc::new(Fdb::open(Some(&config), None).expect("failed to create handle"));

    // Pre-archive some data first
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let grib_path = PathBuf::from(manifest_dir).join("tests/fixtures/template.grib");
    let grib_data = Arc::new(fs::read(&grib_path).expect("failed to read template.grib"));

    // Archive initial data
    let key = Key::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper")
        .with("date", "20230508")
        .with("time", "1200")
        .with("type", "fc")
        .with("levtype", "sfc")
        .with("step", "0")
        .with("param", "151130");
    fdb.archive(&key, &grib_data)
        .expect("initial archive failed");
    fdb.flush().expect("initial flush failed");

    // Spawn threads that mix read and write operations
    let thread_count = 8;
    let iterations = 10;

    let handles: Vec<_> = (0..thread_count)
        .map(|thread_id| {
            let fdb = Arc::clone(&fdb);
            let grib_data = Arc::clone(&grib_data);
            thread::spawn(move || {
                let request = Request::new().with("class", "rd").with("expver", "xxxx");

                for i in 0..iterations {
                    if thread_id % 2 == 0 {
                        // Even threads: read operations
                        let _ = fdb.list(
                            &request,
                            ListOptions {
                                depth: 1,
                                deduplicate: false,
                            },
                        );
                        let _ = fdb.axes(&request, 1);
                    } else {
                        // Odd threads: write operations
                        let step = format!("{}", 1000 + thread_id * 100 + i);
                        let key = Key::new()
                            .with("class", "rd")
                            .with("expver", "xxxx")
                            .with("stream", "oper")
                            .with("date", "20230508")
                            .with("time", "1200")
                            .with("type", "fc")
                            .with("levtype", "sfc")
                            .with("step", &step)
                            .with("param", "151130");

                        let _ = fdb.archive(&key, &grib_data);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked during mixed operations");
    }

    // Final flush
    fdb.flush().expect("final flush failed");
}
