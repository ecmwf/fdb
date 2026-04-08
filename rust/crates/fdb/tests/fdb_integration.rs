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
}

/// Test that `axes()` and `axes_iter()` return the same set of axis names.
/// This is a regression test for the fix that removed hardcoded axis names.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_axes_consistency() {
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

    let request = Request::new().with("class", "rd").with("expver", "xxxx");

    // Get axes via the direct function
    let axes_direct = fdb.axes(&request, 3).expect("failed to get axes");
    let direct_keys: std::collections::HashSet<_> = axes_direct.keys().cloned().collect();

    // Get axes via the iterator
    let axes_iter_items: Vec<_> = fdb
        .axes_iter(&request, 3)
        .expect("failed to get axes iterator")
        .filter_map(std::result::Result::ok)
        .collect();

    // Collect all axis names from iterator
    let iter_keys: std::collections::HashSet<_> = axes_iter_items
        .iter()
        .flat_map(|elem| elem.axes.keys().cloned())
        .collect();

    println!(
        "axes() returned {} axis names: {:?}",
        direct_keys.len(),
        direct_keys
    );
    println!(
        "axes_iter() returned {} axis names: {:?}",
        iter_keys.len(),
        iter_keys
    );

    // Both methods should return the same set of axis names
    assert_eq!(
        direct_keys, iter_keys,
        "axes() and axes_iter() should return the same axis names"
    );

    // Verify we got the expected axes from the archived data
    assert!(direct_keys.contains("class"), "should have 'class' axis");
    assert!(direct_keys.contains("expver"), "should have 'expver' axis");
    assert!(direct_keys.contains("stream"), "should have 'stream' axis");
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
    assert!(!dump_items.is_empty(), "expected at least one dump element");

    // Verify all items are Ok
    let ok_items: Vec<_> = dump_items.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(
        ok_items.len(),
        dump_items.len(),
        "all dump items should be Ok"
    );

    for item in &ok_items {
        println!("  {}", item.content);
        assert!(!item.content.is_empty(), "dump content should not be empty");
    }
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
    assert!(
        !status_items.is_empty(),
        "expected at least one status element"
    );

    // Verify all items are Ok and have valid locations
    for item in &status_items {
        let elem = item.as_ref().expect("status item should be Ok");
        println!("  location={}, status={:?}", elem.location, elem.status);
        assert!(
            !elem.location.is_empty(),
            "status location should not be empty"
        );
    }
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
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_enabled() {
    use fdb::ControlIdentifier;

    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Check if various identifiers are enabled
    let retrieve_enabled = fdb.enabled(ControlIdentifier::Retrieve);
    let archive_enabled = fdb.enabled(ControlIdentifier::Archive);
    let list_enabled = fdb.enabled(ControlIdentifier::List);

    println!(
        "Enabled: retrieve={retrieve_enabled}, archive={archive_enabled}, list={list_enabled}"
    );

    // By default, these should all be enabled
    assert!(retrieve_enabled, "expected retrieve to be enabled");
    assert!(archive_enabled, "expected archive to be enabled");
    assert!(list_enabled, "expected list to be enabled");
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
    let pos = reader
        .seek(SeekFrom::Start(10))
        .expect("seek to start+10 failed");
    assert_eq!(pos, 10);
    assert_eq!(reader.tell(), 10);

    // Test SeekFrom::Current (positive)
    let pos = reader
        .seek(SeekFrom::Current(5))
        .expect("seek current+5 failed");
    assert_eq!(pos, 15);
    assert_eq!(reader.tell(), 15);

    // Test SeekFrom::Current (negative)
    let pos = reader
        .seek(SeekFrom::Current(-5))
        .expect("seek current-5 failed");
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
    reader
        .seek(SeekFrom::Start(0))
        .expect("rewind before read_all failed");
    let all_data = reader.read_all().expect("read_all failed");
    assert_eq!(all_data.len(), grib_data.len());
    assert_eq!(all_data, grib_data);

    // Test negative position errors
    reader.seek(SeekFrom::Start(0)).expect("rewind failed");
    let err = reader.seek(SeekFrom::Current(-100));
    assert!(
        err.is_err(),
        "expected error when seeking to negative position"
    );

    let err = reader.seek(SeekFrom::End(-(total_size.cast_signed() + 100)));
    assert!(
        err.is_err(),
        "expected error when seeking before start via End"
    );

    // Test close() explicitly
    reader.close().expect("close failed");
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
        .filter_map(std::result::Result::ok)
        .collect();

    assert!(!items.is_empty(), "expected at least one item");

    for item in &items {
        // full_key should combine db_key, index_key, and datum_key
        let full = item.full_key();

        // Check that full_key contains entries from all levels
        let total_expected = item.db_key.len() + item.index_key.len() + item.datum_key.len();
        assert_eq!(
            full.len(),
            total_expected,
            "full_key should combine all key levels"
        );

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
        println!("ListElement full_key: {full:?}");
    }
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
    let identifiers = [
        fdb::ControlIdentifier::Retrieve,
        fdb::ControlIdentifier::Archive,
    ];

    // Test None action (query current state)
    let none_result = fdb.control(&request, ControlAction::None, &identifiers);
    assert!(none_result.is_ok(), "control None should succeed");
    let elements: Vec<_> = none_result
        .expect("control None failed")
        .filter_map(std::result::Result::ok)
        .collect();
    println!("Control None elements: {elements:?}");
    assert!(!elements.is_empty(), "control None should return elements");

    // Test Disable action
    let disable_result = fdb.control(&request, ControlAction::Disable, &identifiers);
    assert!(disable_result.is_ok(), "control Disable should succeed");
    let elements: Vec<_> = disable_result
        .expect("control Disable failed")
        .filter_map(std::result::Result::ok)
        .collect();
    println!("Control Disable elements: {elements:?}");

    // Test Enable action
    let enable_result = fdb.control(&request, ControlAction::Enable, &identifiers);
    assert!(enable_result.is_ok(), "control Enable should succeed");
    let elements: Vec<_> = enable_result
        .expect("control Enable failed")
        .filter_map(std::result::Result::ok)
        .collect();
    for elem in &elements {
        println!(
            "Control element - location: {}, identifiers: {:?}",
            elem.location, elem.identifiers
        );
        assert!(
            !elem.location.is_empty(),
            "control element location should not be empty"
        );
    }
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_config_accessors() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Test config_string - try to get a string config value
    let type_str = fdb.config_string("type");
    println!("config_string('type') = {type_str:?}");

    // Test config_int - returns None if key doesn't exist
    let some_int = fdb.config_int("nonexistent_key");
    assert!(some_int.is_none(), "nonexistent key should return None");
    println!("config_int('nonexistent_key') = {some_int:?}");

    // Test config_bool - returns None if key doesn't exist
    let some_bool = fdb.config_bool("nonexistent_key");
    assert!(some_bool.is_none(), "nonexistent key should return None");
    println!("config_bool('nonexistent_key') = {some_bool:?}");

    // Test config_has for various keys
    let has_type = fdb.config_has("type");
    let has_schema = fdb.config_has("schema");
    let has_nonexistent = fdb.config_has("definitely_not_a_key");
    println!("config_has: type={has_type}, schema={has_schema}, nonexistent={has_nonexistent}");
    assert!(!has_nonexistent, "nonexistent key should return false");
}

#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_enabled_identifiers() {
    use fdb::ControlIdentifier;

    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Test enabled() for various identifiers
    let retrieve_enabled = fdb.enabled(ControlIdentifier::Retrieve);
    let archive_enabled = fdb.enabled(ControlIdentifier::Archive);
    let list_enabled = fdb.enabled(ControlIdentifier::List);
    let wipe_enabled = fdb.enabled(ControlIdentifier::Wipe);

    println!(
        "enabled: retrieve={retrieve_enabled}, archive={archive_enabled}, list={list_enabled}, wipe={wipe_enabled}"
    );

    // By default, these operations should be enabled
    assert!(retrieve_enabled, "retrieve should be enabled by default");
    assert!(archive_enabled, "archive should be enabled by default");
    assert!(list_enabled, "list should be enabled by default");
    // wipe may or may not be enabled depending on config
}

// =============================================================================
// Tests for previously untested methods (H9)
// =============================================================================

/// Test `archive_raw()` - archives GRIB data with embedded metadata key.
/// This is useful when archiving GRIB files that already contain full metadata.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_archive_raw() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Read GRIB data with embedded MARS metadata. `synth11.grib` carries
    // section-1 headers (class=od, expver=0001, stream=oper, date=20230508,
    // time=1200, type=fc, levtype=sfc, param=151130, step=1) which is what
    // `archive_raw` extracts to build the storage key.
    let grib_path = fixtures_dir().join("synth11.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read synth11.grib");

    // Archive using archive_raw - key is extracted from GRIB metadata.
    fdb.archive_raw(&grib_data).expect("archive_raw failed");
    fdb.flush().expect("flush failed");

    // Verify the data actually landed in the database by listing it back
    // with the exact key the GRIB embeds, and check the field-level entry
    // matches.
    let request = Request::new().with("class", "od").with("expver", "0001");
    let items: Vec<_> = fdb
        .list(&request, 3, false)
        .expect("failed to list")
        .collect::<Result<_, _>>()
        .expect("list iterator returned an error");

    assert_eq!(
        items.len(),
        1,
        "expected exactly one entry after archive_raw, got {}: {items:#?}",
        items.len()
    );

    let item = &items[0];
    // Spot-check the key parts from each level — these come from the GRIB
    // section-1 headers, so if any drift the test will catch it loudly.
    let db: std::collections::HashMap<_, _> = item.db_key.iter().cloned().collect();
    assert_eq!(db.get("class").map(String::as_str), Some("od"));
    assert_eq!(db.get("expver").map(String::as_str), Some("0001"));
    assert_eq!(db.get("stream").map(String::as_str), Some("oper"));
    assert_eq!(db.get("date").map(String::as_str), Some("20230508"));
    assert_eq!(db.get("time").map(String::as_str), Some("1200"));

    let index: std::collections::HashMap<_, _> = item.index_key.iter().cloned().collect();
    assert_eq!(index.get("type").map(String::as_str), Some("fc"));
    assert_eq!(index.get("levtype").map(String::as_str), Some("sfc"));

    let datum: std::collections::HashMap<_, _> = item.datum_key.iter().cloned().collect();
    assert_eq!(datum.get("param").map(String::as_str), Some("151130"));
    assert_eq!(datum.get("step").map(String::as_str), Some("1"));

    // The byte length recorded in the listing should match the GRIB message
    // we archived (proves it's not a zero-length sentinel).
    assert_eq!(item.length, grib_data.len() as u64);
}

/// Test `read_uri()` - reads data from a specific URI location.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_read_uri() {
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

    // List to get the URI
    let request = Request::new().with("class", "rd").with("expver", "xxxx");
    let items: Vec<_> = fdb
        .list(&request, 3, false)
        .expect("failed to list")
        .filter_map(std::result::Result::ok)
        .collect();

    assert!(!items.is_empty(), "expected at least one item");

    // Get the URI from the first list element
    let uri = &items[0].uri;
    let offset = items[0].offset;
    let length = items[0].length;
    println!("Reading from URI: {uri} (offset={offset}, length={length})");

    // Read using the URI
    let mut reader = fdb.read_uri(uri).expect("failed to read_uri");

    // Seek to the offset and read the data
    reader.seek_to(offset).expect("failed to seek");
    let mut data = vec![0u8; usize::try_from(length).expect("length exceeds usize::MAX")];
    reader.read_exact(&mut data).expect("failed to read");

    assert_eq!(
        data.len(),
        grib_data.len(),
        "read data should match original size"
    );
    assert_eq!(data, grib_data, "read data should match original");
}

/// Test `read_uris()` - reads data from multiple URI locations.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_read_uris() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive multiple pieces of data
    let grib_path = fixtures_dir().join("template.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read template.grib");

    // Archive with different steps
    for step in ["0", "1", "2"] {
        let key = Key::new()
            .with("class", "rd")
            .with("expver", "xxxx")
            .with("stream", "oper")
            .with("date", "20230508")
            .with("time", "1200")
            .with("type", "fc")
            .with("levtype", "sfc")
            .with("step", step)
            .with("param", "151130");

        fdb.archive(&key, &grib_data).expect("failed to archive");
    }
    fdb.flush().expect("flush failed");

    // List to get URIs
    let request = Request::new().with("class", "rd").with("expver", "xxxx");
    let items: Vec<_> = fdb
        .list(&request, 3, false)
        .expect("failed to list")
        .filter_map(std::result::Result::ok)
        .collect();

    assert!(items.len() >= 2, "expected at least 2 items");

    // Collect URIs (with offset/length encoded if needed)
    // Note: read_uris expects URIs that include offset/length or full file URIs
    let uris: Vec<String> = items.iter().take(2).map(|item| item.uri.clone()).collect();
    println!("Reading from {} URIs", uris.len());

    // Read using multiple URIs
    let mut reader = fdb.read_uris(&uris, false).expect("failed to read_uris");

    // Read all data
    let data = reader.read_all().expect("failed to read_all");
    println!("read_uris returned {} bytes", data.len());

    // Should have read data from both URIs
    assert!(!data.is_empty(), "expected non-empty data from read_uris");
}

/// Test `read_from_list()` - reads data from a `ListIterator`.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_read_from_list() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive data
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

    // Get a list iterator
    let request = Request::new().with("class", "rd").with("expver", "xxxx");
    let list_iter = fdb.list(&request, 3, false).expect("failed to list");

    // Read from the list iterator
    let mut reader = fdb
        .read_from_list(list_iter, false)
        .expect("failed to read_from_list");

    // Read all data
    let data = reader.read_all().expect("failed to read_all");
    println!("read_from_list returned {} bytes", data.len());

    assert_eq!(
        data.len(),
        grib_data.len(),
        "read_from_list should return same amount of data"
    );
    assert_eq!(data, grib_data, "data should match original");
}

/// Test `move_data()` - moves data to a new location.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_move_data() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    // Create a destination directory within tmpdir
    let dest_dir = tmpdir.path().join("dest");
    fs::create_dir(&dest_dir).expect("failed to create dest dir");

    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB from YAML");

    // Archive data
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

    // Move data to new location
    let request = Request::new().with("class", "rd").with("expver", "xxxx");
    let dest_path = dest_dir.to_str().expect("invalid path");

    let result = fdb.move_data(&request, dest_path);
    println!(
        "move_data result: {}",
        if result.is_ok() { "Ok" } else { "Err" }
    );

    // Collect move elements if successful
    if let Ok(move_iter) = result {
        let elements: Vec<_> = move_iter.filter_map(std::result::Result::ok).collect();
        println!("move_data returned {} elements", elements.len());
        for elem in &elements {
            println!("  moved: {} -> {}", elem.source, elem.destination);
        }
    }

    // Note: move_data behavior depends on FDB configuration and backend support.
    // The test verifies the API works without panicking.
}

/// Walk a directory tree and collect every `toc.*` filename (subtoc files
/// produced by `useSubToc: true`). Returns the relative basenames so the test
/// only sees the discriminating part of the layout.
fn collect_subtoc_files(root: &std::path::Path) -> Vec<String> {
    fn walk(dir: &std::path::Path, out: &mut Vec<String>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                // Subtoc files are produced by `eckit::PathName::unique("toc")`
                // and have the form `toc.<unique-suffix>`. Exclude the main
                // `toc` file itself.
                if name.starts_with("toc.") {
                    out.push(name.to_string());
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(root, &mut out);
    out
}

/// Drive an archive + retrieve cycle and return the subtoc files that ended
/// up in `tmpdir`. Used by the subtoc on/off test below.
fn archive_one_record(fdb: &Fdb) {
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
}

/// Verify that the `useSubToc` user-config flag is actually plumbed through
/// `fdb5::Config`'s second constructor argument: with the flag off the
/// database directory contains only the main `toc`, with the flag on we get
/// at least one `toc.<unique>` subtoc file in the same place.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_subtoc_user_config() {
    // --- subtocs OFF (default) ---
    let tmpdir_off = tempfile::tempdir().expect("failed to create temp dir");
    let config_off = create_test_config(tmpdir_off.path());
    {
        let fdb_off = Fdb::from_yaml_with_user_config(&config_off, "useSubToc: false")
            .expect("from_yaml off");
        archive_one_record(&fdb_off);
    } // drop handle so the TOC is fully closed before we walk the dir

    let subtocs_off = collect_subtoc_files(tmpdir_off.path());
    assert!(
        subtocs_off.is_empty(),
        "expected no subtoc files with useSubToc=false, found: {subtocs_off:?}"
    );

    // --- subtocs ON ---
    let tmpdir_on = tempfile::tempdir().expect("failed to create temp dir");
    let config_on = create_test_config(tmpdir_on.path());
    {
        let fdb_on =
            Fdb::from_yaml_with_user_config(&config_on, "useSubToc: true").expect("from_yaml on");
        archive_one_record(&fdb_on);
    }

    let subtocs_on = collect_subtoc_files(tmpdir_on.path());
    assert!(
        !subtocs_on.is_empty(),
        "expected at least one subtoc file with useSubToc=true, found none under {}",
        tmpdir_on.path().display()
    );
}

/// Smoke test for the `preloadTocBTree` user-config flag.
///
/// Unlike `useSubToc`, this option only changes runtime behaviour (it eagerly
/// loads the toc B-tree on open instead of lazily) and produces no observable
/// on-disk artifact, so we can only verify that both values are accepted by
/// the C++ side and that an archive + list round-trip succeeds in each mode.
#[test]
#[ignore = "requires FDB libraries"]
fn test_fdb_preload_toc_btree_user_config() {
    for preload in ["true", "false"] {
        let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
        let config = create_test_config(tmpdir.path());
        let user_config = format!("preloadTocBTree: {preload}");

        let fdb = Fdb::from_yaml_with_user_config(&config, &user_config)
            .unwrap_or_else(|e| panic!("from_yaml_with_user_config({user_config:?}) failed: {e}"));

        archive_one_record(&fdb);

        let request = Request::new().with("class", "rd").with("expver", "xxxx");
        let items: Vec<_> = fdb
            .list(&request, 3, false)
            .expect("failed to list")
            .collect();
        assert!(
            !items.is_empty(),
            "list returned no items with preloadTocBTree={preload}"
        );
    }
}
