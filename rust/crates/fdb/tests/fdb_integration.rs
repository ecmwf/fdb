//! Integration tests for FDB safe wrapper.
//!
//! These tests require FDB to be properly initialized and are marked with `#[ignore]`
//! by default.
//!
//! Run with: `cargo test --test fdb_integration -- --ignored --test-threads=1`
//!
//! Note: `--test-threads=1` is recommended when running with gribjump tests that modify
//! the global `FDB5_CONFIG` environment variable.

use std::env;
use std::fs;
use std::io::Read;
use std::path::PathBuf;

use fdb::{Fdb, Key, Request};

/// Get the path to test fixtures directory.
fn fixtures_dir() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest_dir).join("tests/fixtures")
}

/// Create a temporary FDB configuration for testing.
fn create_test_config(tmpdir: &std::path::Path) -> String {
    // Copy schema to temp directory
    let schema_src = fixtures_dir().join("schema");
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

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_version() {
    let version = Fdb::version();
    assert!(!version.is_empty());
    println!("FDB version: {version}");
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_git_sha1() {
    let sha = Fdb::git_sha1();
    assert!(!sha.is_empty());
    println!("FDB git SHA1: {sha}");
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_handle_from_yaml() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());
    println!("Config:\n{config}");

    let fdb = Fdb::from_yaml(&config);
    assert!(fdb.is_ok(), "failed to create FDB handle: {:?}", fdb.err());

    // Keep tmpdir alive until FDB is dropped
    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_key_creation() {
    let key = Key::new().with("class", "rd").with("expver", "xxxx");
    assert_eq!(key.len(), 2);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_request_creation() {
    let request = Request::new().with("class", "rd").with("expver", "xxxx");
    assert_eq!(request.len(), 2);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_list_no_results() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Request with criteria that won't match anything (FDB requires at least one criterion)
    let request = Request::new().with("class", "nonexistent");

    let items: Vec<_> = fdb
        .list(&request, 3, false)
        .expect("failed to list")
        .collect();

    assert!(
        items.is_empty(),
        "expected no results for nonexistent class"
    );

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_archive_simple() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());
    println!("Temp dir: {}", tmpdir.path().display());
    println!("Config:\n{config}");

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Read test GRIB data
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");
    println!("GRIB data size: {} bytes", grib_data.len());

    // Create key matching schema: class, expver, stream, date, time, type, levtype, step, param
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

    println!("Archiving...");
    let result = fdb.archive(&key, &grib_data);
    println!("Archive result: {result:?}");

    if result.is_ok() {
        println!("Flushing...");
        fdb.flush().expect("flush failed");
        println!("Done!");
    }

    // Keep tmpdir alive
    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_archive_retrieve_cycle() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // List with partial query
    let list_request = Request::new().with("class", "rd").with("expver", "xxxx");

    let items: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();

    println!("Listed {} items", items.len());
    assert!(!items.is_empty(), "no items found after archive");

    // Retrieve with fully-specified request (FDB needs exact match for retrieve)
    let retrieve_request = Request::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper")
        .with("date", "20230508")
        .with("time", "1200")
        .with("type", "fc")
        .with("levtype", "sfc")
        .with("step", "0")
        .with("param", "151130");

    let mut reader = fdb.retrieve(&retrieve_request).expect("failed to retrieve");
    let mut retrieved_data = Vec::new();
    reader
        .read_to_end(&mut retrieved_data)
        .expect("failed to read");

    assert_eq!(retrieved_data.len(), grib_data.len());

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_axes() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive some data first
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // Query axes
    let request = Request::new().with("class", "rd").with("expver", "xxxx");
    let axes = fdb.axes(&request, 3).expect("failed to get axes");

    println!("Axes: {axes:?}");

    // Should have some axes returned
    assert!(!axes.is_empty(), "expected at least one axis");

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_axes_iterator() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive some data first
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // Query axes via iterator
    let request = Request::new().with("class", "rd").with("expver", "xxxx");
    let axes_items: Vec<_> = fdb
        .axes_iter(&request, 3)
        .expect("failed to get axes iterator")
        .collect();

    println!("Axes iterator returned {} items", axes_items.len());

    for item in &axes_items {
        match item {
            Ok(elem) => println!("  db_key={:?}, axes={:?}", elem.db_key, elem.axes),
            Err(e) => println!("  error: {e}"),
        }
    }

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_dump() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive some data first
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // Dump database structure
    let request = Request::new().with("class", "rd");
    let dump_items: Vec<_> = fdb.dump(&request, true).expect("failed to dump").collect();

    println!("Dump returned {} items", dump_items.len());
    for item in &dump_items {
        match item {
            Ok(elem) => println!("  {}", elem.content),
            Err(e) => println!("  error: {e}"),
        }
    }

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_status() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive some data first
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // Get status
    let request = Request::new().with("class", "rd");
    let status_items: Vec<_> = fdb
        .status(&request)
        .expect("failed to get status")
        .collect();

    println!("Status returned {} items", status_items.len());
    for item in &status_items {
        match item {
            Ok(elem) => println!("  location={}, status={:?}", elem.location, elem.status),
            Err(e) => println!("  error: {e}"),
        }
    }

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_wipe_dry_run() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive some data first
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // Verify data exists
    let list_request = Request::new().with("class", "rd");
    let items_before: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();
    assert!(
        !items_before.is_empty(),
        "expected data to exist before wipe"
    );

    // Dry-run wipe (doit=false)
    let wipe_request = Request::new().with("class", "rd").with("expver", "xxxx");
    let wipe_items: Vec<_> = fdb
        .wipe(&wipe_request, false, false, false)
        .expect("failed to wipe")
        .collect();

    println!("Wipe dry-run returned {} items", wipe_items.len());
    for item in &wipe_items {
        match item {
            Ok(elem) => println!("  would wipe: {}", elem.content),
            Err(e) => println!("  error: {e}"),
        }
    }

    // Verify data still exists after dry-run
    let items_after: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();
    assert_eq!(
        items_before.len(),
        items_after.len(),
        "dry-run should not delete data"
    );

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_purge_dry_run() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive same data twice to create duplicates
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");
    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // Dry-run purge (doit=false)
    let purge_request = Request::new().with("class", "rd");
    let purge_items: Vec<_> = fdb
        .purge(&purge_request, false, false)
        .expect("failed to purge")
        .collect();

    println!("Purge dry-run returned {} items", purge_items.len());
    for item in &purge_items {
        match item {
            Ok(elem) => println!("  would purge: {}", elem.content),
            Err(e) => println!("  error: {e}"),
        }
    }

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_stats_iterator() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive some data
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // Get stats
    let request = Request::new().with("class", "rd");
    let stats_items: Vec<_> = fdb
        .stats_iter(&request)
        .expect("failed to get stats")
        .collect();

    println!("Stats returned {} items", stats_items.len());
    for item in &stats_items {
        match item {
            Ok(elem) => println!(
                "  fields={}, size={}, duplicates={}",
                elem.field_count, elem.total_size, elem.duplicate_count
            ),
            Err(e) => println!("  error: {e}"),
        }
    }

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_dirty_flag() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Initially not dirty
    assert!(!fdb.dirty(), "expected FDB to not be dirty initially");

    // Archive some data
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");

    // Should be dirty after archive
    assert!(fdb.dirty(), "expected FDB to be dirty after archive");

    // Flush
    fdb.flush().expect("flush failed");

    // Should not be dirty after flush
    assert!(!fdb.dirty(), "expected FDB to not be dirty after flush");

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_config_methods() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Test config()
    let cfg = fdb.config();
    println!(
        "Config: schema_path={}, config_path={}",
        cfg.schema_path, cfg.config_path
    );

    // Test id() and name()
    let id = fdb.id();
    let name = fdb.name();
    println!("FDB id={id}, name={name}");
    assert!(!name.is_empty(), "expected non-empty FDB name");

    // Test config_has
    // Note: available keys depend on the configuration
    let has_type = fdb.config_has("type");
    println!("config_has('type') = {has_type}");

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_aggregate_stats() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Initial stats
    let stats_before = fdb.stats();
    println!(
        "Stats before: archive={}, location={}, flush={}",
        stats_before.num_archive, stats_before.num_location, stats_before.num_flush
    );

    // Archive some data
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");

    // Stats after archive
    let stats_after_archive = fdb.stats();
    println!(
        "Stats after archive: archive={}, location={}, flush={}",
        stats_after_archive.num_archive,
        stats_after_archive.num_location,
        stats_after_archive.num_flush
    );
    assert!(
        stats_after_archive.num_archive > stats_before.num_archive,
        "expected archive count to increase"
    );

    fdb.flush().expect("flush failed");

    // Stats after flush
    let stats_after_flush = fdb.stats();
    println!(
        "Stats after flush: archive={}, location={}, flush={}",
        stats_after_flush.num_archive, stats_after_flush.num_location, stats_after_flush.num_flush
    );
    assert!(
        stats_after_flush.num_flush > stats_after_archive.num_flush,
        "expected flush count to increase"
    );

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_enabled() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Check if various identifiers are enabled
    let retrieve_enabled = fdb.enabled("retrieve");
    let archive_enabled = fdb.enabled("archive");
    let list_enabled = fdb.enabled("list");

    println!(
        "Enabled: retrieve={retrieve_enabled}, archive={archive_enabled}, list={list_enabled}"
    );

    // By default, these should all be enabled
    assert!(retrieve_enabled, "expected retrieve to be enabled");
    assert!(archive_enabled, "expected archive to be enabled");
    assert!(list_enabled, "expected list to be enabled");

    drop(fdb);
    drop(tmpdir);
}

/// Test matching C++ `test_callback.cc`: Archive and flush callback
/// Archives multiple keys and verifies callbacks are called for each.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_callbacks() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Set up callback tracking (matching C++ test_callback.cc)
    let flush_called = Arc::new(AtomicBool::new(false));
    let archive_count = Arc::new(AtomicUsize::new(0));

    // Register flush callback
    let flush_called_clone = Arc::clone(&flush_called);
    fdb.on_flush(move || {
        flush_called_clone.store(true, Ordering::SeqCst);
    });

    // Register archive callback
    let archive_count_clone = Arc::clone(&archive_count);
    fdb.on_archive(move |data| {
        archive_count_clone.fetch_add(1, Ordering::SeqCst);
        println!("Archive callback: key has {} entries", data.key.len());
    });

    // Archive data - matching C++ test which archives 3 keys
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

    // First key
    let key1 = Key::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper")
        .with("date", "20101010")
        .with("time", "0000")
        .with("type", "fc")
        .with("levtype", "sfc")
        .with("step", "1")
        .with("param", "130");
    fdb.archive(&key1, &grib_data).expect("failed to archive");

    // Second key (different date)
    let key2 = Key::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper")
        .with("date", "20111213")
        .with("time", "0000")
        .with("type", "fc")
        .with("levtype", "sfc")
        .with("step", "1")
        .with("param", "130");
    fdb.archive(&key2, &grib_data).expect("failed to archive");

    // Third key (different type)
    let key3 = Key::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper")
        .with("date", "20111213")
        .with("time", "0000")
        .with("type", "an")
        .with("levtype", "sfc")
        .with("step", "1")
        .with("param", "130");
    fdb.archive(&key3, &grib_data).expect("failed to archive");

    fdb.flush().expect("flush failed");

    // Verify callbacks were called (matching C++ EXPECT assertions)
    assert!(
        flush_called.load(Ordering::SeqCst),
        "expected flush callback to be called"
    );
    assert_eq!(
        archive_count.load(Ordering::SeqCst),
        3,
        "expected archive callback to be called 3 times"
    );

    println!(
        "Callbacks: flush_called={}, archive_count={}",
        flush_called.load(Ordering::SeqCst),
        archive_count.load(Ordering::SeqCst)
    );

    drop(fdb);
    drop(tmpdir);
}

/// Test matching C++ `test_wipe.cc`: Actual wipe (doit=true)
/// Archives data to multiple databases, then wipes them.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_wipe_actual() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

    // Archive to first database (class=rd, expver=xxxx)
    let key1 = Key::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper")
        .with("date", "20230508")
        .with("time", "1200")
        .with("type", "fc")
        .with("levtype", "sfc")
        .with("step", "0")
        .with("param", "151130");
    fdb.archive(&key1, &grib_data).expect("failed to archive");

    // Archive to second database (class=rd, expver=yyyy)
    let key2 = Key::new()
        .with("class", "rd")
        .with("expver", "yyyy")
        .with("stream", "oper")
        .with("date", "20230508")
        .with("time", "1200")
        .with("type", "fc")
        .with("levtype", "sfc")
        .with("step", "0")
        .with("param", "151130");
    fdb.archive(&key2, &grib_data).expect("failed to archive");

    fdb.flush().expect("flush failed");
    println!("Archived 2 fields to 2 databases");

    // Verify FDB is populated
    let list_request = Request::new().with("class", "rd");
    let items: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();
    assert_eq!(items.len(), 2, "expected 2 fields");
    println!("Listed {} fields", items.len());

    // Wipe first database (doit=true)
    let wipe_request1 = Request::new().with("class", "rd").with("expver", "xxxx");
    let wipe_items: Vec<_> = fdb
        .wipe(&wipe_request1, true, false, false)
        .expect("failed to wipe")
        .collect();
    println!("Wipe returned {} items", wipe_items.len());

    // Verify first database is wiped
    let items_after: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();
    assert_eq!(items_after.len(), 1, "expected 1 field after wipe");
    println!("Listed {} fields after wipe", items_after.len());

    // Wipe remaining database
    let wipe_request2 = Request::new().with("class", "rd");
    let _: Vec<_> = fdb
        .wipe(&wipe_request2, true, false, false)
        .expect("failed to wipe")
        .collect();

    // Verify all data is wiped
    let items_final: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();
    assert_eq!(items_final.len(), 0, "expected 0 fields after full wipe");
    println!("Wiped all databases");

    drop(fdb);
    drop(tmpdir);
}

/// Test matching C++ `test_wipe.cc`: Wipe masked data (duplicates)
/// Archives same key multiple times, then wipes.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_wipe_masked_data() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    // Archive same key twice (creates masked/duplicate data)
    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");
    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");
    println!("Archived 2 fields (1 masked)");

    // List including masked
    let list_request = Request::new().with("class", "rd");
    let items_with_masked: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();
    println!("Listed {} fields including masked", items_with_masked.len());

    // List excluding masked (deduplicate=true)
    let items_dedup: Vec<_> = fdb
        .list(&list_request, 3, true)
        .expect("failed to list")
        .collect();
    println!("Listed {} fields excluding masked", items_dedup.len());
    assert_eq!(items_dedup.len(), 1, "expected 1 field when deduplicated");

    // Wipe all
    let wipe_request = Request::new().with("class", "rd").with("expver", "xxxx");
    let wipe_items: Vec<_> = fdb
        .wipe(&wipe_request, true, false, false)
        .expect("failed to wipe")
        .collect();
    println!("Wipe returned {} items", wipe_items.len());

    // Verify all wiped
    let items_final: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();
    assert_eq!(items_final.len(), 0, "expected 0 fields after wipe");

    drop(fdb);
    drop(tmpdir);
}

/// Test matching C++ `test_wipe.cc`: Purge removes duplicates
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_purge_actual() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    // Archive same key twice (creates duplicate)
    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");
    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");
    println!("Archived 2 fields (1 duplicate)");

    // List including masked
    let list_request = Request::new().with("class", "rd");
    let items_before: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();
    println!("Listed {} fields before purge", items_before.len());

    // Purge duplicates (doit=true)
    let purge_request = Request::new().with("class", "rd");
    let purge_items: Vec<_> = fdb
        .purge(&purge_request, true, false)
        .expect("failed to purge")
        .collect();
    println!("Purge returned {} items", purge_items.len());

    // List after purge - should have only 1 field
    let items_after: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .collect();
    println!("Listed {} fields after purge", items_after.len());
    assert_eq!(
        items_after.len(),
        1,
        "expected 1 field after purge removes duplicates"
    );

    drop(fdb);
    drop(tmpdir);
}

/// Test matching C++ `test_config.cc`: Config expansion from YAML
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_config_from_yaml() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");

    // Copy schema to temp directory
    let schema_src = fixtures_dir().join("schema");
    let schema_dst = tmpdir.path().join("schema");
    fs::copy(&schema_src, &schema_dst).expect("failed to copy schema");

    // Create YAML config (matching C++ test_config.cc format)
    let config = format!(
        r"---
type: local
engine: toc
schema: {}/schema
spaces:
  - roots:
      - path: {}
",
        tmpdir.path().display(),
        tmpdir.path().display()
    );

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Verify config was parsed
    let name = fdb.name();
    assert!(!name.is_empty(), "expected non-empty FDB name");
    println!("FDB type/name: {name}");

    // Test config accessors
    let has_type = fdb.config_has("type");
    println!("config_has('type') = {has_type}");

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_datareader_seek() {
    use std::io::{Read as IoRead, Seek as IoSeek, SeekFrom};

    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive data first
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // Retrieve to get a DataReader
    let retrieve_request = Request::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper")
        .with("date", "20230508")
        .with("time", "1200")
        .with("type", "fc")
        .with("levtype", "sfc")
        .with("step", "0")
        .with("param", "151130");

    let mut reader = fdb.retrieve(&retrieve_request).expect("failed to retrieve");

    // Test size() and tell()
    let total_size = reader.size();
    assert!(total_size > 0, "expected non-zero size");
    assert_eq!(reader.tell(), 0, "expected initial position at 0");

    // Test SeekFrom::Start
    let pos = reader.seek(SeekFrom::Start(10)).expect("seek to start+10 failed");
    assert_eq!(pos, 10);
    assert_eq!(reader.tell(), 10);

    // Test SeekFrom::Current (positive)
    let pos = reader.seek(SeekFrom::Current(5)).expect("seek current+5 failed");
    assert_eq!(pos, 15);
    assert_eq!(reader.tell(), 15);

    // Test SeekFrom::Current (negative)
    let pos = reader.seek(SeekFrom::Current(-5)).expect("seek current-5 failed");
    assert_eq!(pos, 10);
    assert_eq!(reader.tell(), 10);

    // Test SeekFrom::End
    let pos = reader.seek(SeekFrom::End(-10)).expect("seek end-10 failed");
    assert_eq!(pos, total_size - 10);
    assert_eq!(reader.tell(), total_size - 10);

    // Test SeekFrom::End to get to end
    let pos = reader.seek(SeekFrom::End(0)).expect("seek to end failed");
    assert_eq!(pos, total_size);

    // Test SeekFrom::Start to rewind
    let pos = reader.seek(SeekFrom::Start(0)).expect("rewind failed");
    assert_eq!(pos, 0);

    // Test seek_to() method
    reader.seek_to(20).expect("seek_to failed");
    assert_eq!(reader.tell(), 20);

    // Test read after seek
    let mut buf = [0u8; 10];
    let n = reader.read(&mut buf).expect("read after seek failed");
    assert!(n > 0, "expected to read some bytes");

    // Test read_all() reads from current position
    reader.seek(SeekFrom::Start(0)).expect("rewind before read_all failed");
    let all_data = reader.read_all().expect("read_all failed");
    assert_eq!(all_data.len(), grib_data.len());
    assert_eq!(all_data, grib_data);

    // Test negative position errors
    reader.seek(SeekFrom::Start(0)).expect("rewind failed");
    let err = reader.seek(SeekFrom::Current(-100));
    assert!(err.is_err(), "expected error when seeking to negative position");

    let err = reader.seek(SeekFrom::End(-(total_size as i64 + 100)));
    assert!(err.is_err(), "expected error when seeking before start via End");

    // Test close() explicitly
    reader.close().expect("close failed");

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_list_element_full_key() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive data first
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    // List and check full_key()
    let list_request = Request::new().with("class", "rd").with("expver", "xxxx");
    let items: Vec<_> = fdb
        .list(&list_request, 3, false)
        .expect("failed to list")
        .filter_map(|r| r.ok())
        .collect();

    assert!(!items.is_empty(), "expected at least one item");

    for item in &items {
        // full_key should combine db_key, index_key, and datum_key
        let full = item.full_key();

        // Check that full_key contains entries from all levels
        let total_expected = item.db_key.len() + item.index_key.len() + item.datum_key.len();
        assert_eq!(full.len(), total_expected, "full_key should combine all key levels");

        // Verify the ordering: db_key first, then index_key, then datum_key
        let mut idx = 0;
        for (k, v) in &item.db_key {
            assert_eq!(&full[idx], &(k.clone(), v.clone()));
            idx += 1;
        }
        for (k, v) in &item.index_key {
            assert_eq!(&full[idx], &(k.clone(), v.clone()));
            idx += 1;
        }
        for (k, v) in &item.datum_key {
            assert_eq!(&full[idx], &(k.clone(), v.clone()));
            idx += 1;
        }

        // Print for debugging
        println!("ListElement full_key: {:?}", full);
    }

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_control_lock_unlock() {
    use fdb::ControlAction;

    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive data first so we have something to control
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

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

    fdb.archive(&key, &grib_data).expect("failed to archive");
    fdb.flush().expect("flush failed");

    let request = Request::new().with("class", "rd").with("expver", "xxxx");
    let identifiers = vec!["retrieve".to_string(), "archive".to_string()];

    // Test None action (query current state)
    let none_result = fdb.control(&request, ControlAction::None, &identifiers);
    if let Ok(iter) = none_result {
        let elements: Vec<_> = iter.filter_map(|r| r.ok()).collect();
        println!("Control None elements: {:?}", elements);
    }

    // Test Disable action
    let disable_result = fdb.control(&request, ControlAction::Disable, &identifiers);
    if let Ok(iter) = disable_result {
        let elements: Vec<_> = iter.filter_map(|r| r.ok()).collect();
        println!("Control Disable elements: {:?}", elements);
    }

    // Test Enable action
    let enable_result = fdb.control(&request, ControlAction::Enable, &identifiers);
    if let Ok(iter) = enable_result {
        let elements: Vec<_> = iter.filter_map(|r| r.ok()).collect();
        for elem in &elements {
            println!("Control element - location: {}, identifiers: {:?}", elem.location, elem.identifiers);
        }
    }

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_config_accessors() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Test config_string - try to get a string config value
    let type_str = fdb.config_string("type");
    println!("config_string('type') = '{type_str}'");

    // Test config_int - try to get an int config value
    // Note: may return 0 if key doesn't exist or isn't an int
    let some_int = fdb.config_int("nonexistent_key");
    println!("config_int('nonexistent_key') = {some_int}");

    // Test config_bool - try to get a bool config value
    let some_bool = fdb.config_bool("nonexistent_key");
    println!("config_bool('nonexistent_key') = {some_bool}");

    // Test config_has for various keys
    let has_type = fdb.config_has("type");
    let has_schema = fdb.config_has("schema");
    let has_nonexistent = fdb.config_has("definitely_not_a_key");
    println!("config_has: type={has_type}, schema={has_schema}, nonexistent={has_nonexistent}");
    assert!(!has_nonexistent, "nonexistent key should return false");

    drop(fdb);
    drop(tmpdir);
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_enabled_identifiers() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Test enabled() for various identifiers
    let retrieve_enabled = fdb.enabled("retrieve");
    let archive_enabled = fdb.enabled("archive");
    let list_enabled = fdb.enabled("list");
    let wipe_enabled = fdb.enabled("wipe");

    println!("enabled: retrieve={retrieve_enabled}, archive={archive_enabled}, list={list_enabled}, wipe={wipe_enabled}");

    // By default, most operations should be enabled
    // (unless explicitly disabled in config)

    drop(fdb);
    drop(tmpdir);
}
