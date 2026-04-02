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
//! Run with: `cargo test --test fdb_thread_safety --features vendored`
//!
//! For integration tests that require FDB libraries:
//! `cargo test --test fdb_thread_safety --features vendored -- --ignored --test-threads=1`

use std::sync::Arc;
use std::thread;

use fdb::{Fdb, Key, Request};

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
#[ignore = "requires FDB libraries and configuration"]
fn test_handle_creation() {
    let fdb = Fdb::new();
    assert!(fdb.is_ok(), "Failed to create Fdb: {:?}", fdb.err());
}

/// Test: `Fdb` can be shared via Arc for concurrent access
#[test]
#[ignore = "requires FDB libraries and configuration"]
fn test_arc_sharing_readonly() {
    let fdb = Arc::new(Fdb::new().expect("failed to create handle"));

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
#[ignore = "requires FDB libraries and configuration"]
fn test_concurrent_readonly_methods() {
    let fdb = Arc::new(Fdb::new().expect("failed to create handle"));

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
#[ignore = "requires FDB libraries and configuration"]
fn test_concurrent_list_operations() {
    let fdb = Arc::new(Fdb::new().expect("failed to create handle"));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let fdb = Arc::clone(&fdb);
            thread::spawn(move || {
                let request = Request::new().with("class", "rd");
                for _ in 0..10 {
                    let _ = fdb.list(&request, 1, false);
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
#[ignore = "requires FDB libraries and configuration"]
fn test_concurrent_axes() {
    let fdb = Arc::new(Fdb::new().expect("failed to create handle"));

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
#[ignore = "requires FDB libraries and configuration"]
fn test_stress_concurrent_access() {
    let fdb = Arc::new(Fdb::new().expect("failed to create handle"));
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
                        let _ = fdb.list(&request, 1, false);
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
#[ignore = "requires FDB libraries and configuration"]
fn test_concurrent_errors_no_crash() {
    let fdb = Arc::new(Fdb::new().expect("failed to create handle"));

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let fdb = Arc::clone(&fdb);
            thread::spawn(move || {
                // Use invalid requests to trigger errors
                let value = format!("value_{i}");
                let request = Request::new().with("INVALID_KEY", &value);
                for _ in 0..20 {
                    // Ignore the error - testing that concurrent errors don't crash
                    let _ = fdb.list(&request, 1, false);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }
}
