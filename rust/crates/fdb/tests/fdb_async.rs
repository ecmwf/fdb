//! Async integration tests for `Fdb`.
//!
//! These tests verify correct concurrent access from multiple tokio tasks.
//!
//! `Fdb` implements `Send + Sync` and uses internal locking. Methods can be
//! called directly on `Arc<Fdb>` without external synchronization.
//!
//! Run with `cargo test --test fdb_async`.

use std::env;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use fdb::{Fdb, Key, ListOptions, Request};
use tokio::task::JoinSet;

/// Get the path to test fixtures directory.
fn fixtures_dir() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest_dir).join("tests/fixtures")
}

/// Create a temporary FDB configuration for testing.
fn create_test_config(tmpdir: &std::path::Path) -> String {
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

/// Build a Request from a Key.
fn request_from_key(key: &Key) -> Request {
    let mut request = Request::new();
    for (k, v) in key.entries() {
        request = request.with(k, v);
    }
    request
}

/// Archive test data and return the key used.
fn archive_test_data(fdb: &Fdb, step: &str) -> Key {
    let grib_data = fs::read(fixtures_dir().join("synth11.grib")).expect("failed to read GRIB");

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

    fdb.archive(&key, &grib_data).expect("archive failed");
    key
}

#[tokio::test]
async fn test_fdb_concurrent_archive() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    // Fdb has internal locking
    let fdb = Arc::new(Fdb::open(Some(&config), None).expect("failed to create FDB"));

    let grib_data =
        Arc::new(fs::read(fixtures_dir().join("synth11.grib")).expect("failed to read GRIB"));

    let mut tasks = JoinSet::new();

    // Spawn multiple tasks that archive data concurrently
    for i in 0..4 {
        let fdb = Arc::clone(&fdb);
        let grib_data = Arc::clone(&grib_data);

        tasks.spawn(async move {
            let key = Key::new()
                .with("class", "rd")
                .with("expver", "xxxx")
                .with("stream", "oper")
                .with("date", "20230508")
                .with("time", "1200")
                .with("type", "fc")
                .with("levtype", "sfc")
                .with("step", &i.to_string())
                .with("param", "151130");

            // Internal locking handles synchronization
            fdb.archive(&key, &grib_data).expect("archive failed");
            i
        });
    }

    // Wait for all tasks to complete
    let mut completed = Vec::new();
    while let Some(result) = tasks.join_next().await {
        completed.push(result.expect("task panicked"));
    }

    assert_eq!(completed.len(), 4);
    println!("Concurrent archive completed: {completed:?}");

    // Flush to persist
    fdb.flush().expect("flush failed");
}

#[tokio::test]
async fn test_fdb_concurrent_retrieve() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Arc::new(Fdb::open(Some(&config), None).expect("failed to create FDB"));

    // Archive some test data first
    for i in 0..4 {
        archive_test_data(&fdb, &i.to_string());
    }
    fdb.flush().expect("flush failed");

    let mut tasks = JoinSet::new();

    // Spawn multiple tasks that retrieve data concurrently
    for i in 0..4 {
        let fdb = Arc::clone(&fdb);

        tasks.spawn(async move {
            let key = Key::new()
                .with("class", "rd")
                .with("expver", "xxxx")
                .with("stream", "oper")
                .with("date", "20230508")
                .with("time", "1200")
                .with("type", "fc")
                .with("levtype", "sfc")
                .with("step", &i.to_string())
                .with("param", "151130");

            let request = request_from_key(&key);

            // Retrieve returns a DataReader that owns the data
            let mut reader = fdb.retrieve(&request).expect("retrieve failed");

            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).expect("read failed");

            (i, buf.len())
        });
    }

    // Collect results
    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        results.push(result.expect("task panicked"));
    }

    assert_eq!(results.len(), 4);
    for (step, size) in &results {
        assert!(*size > 0, "step {step} should have data");
        println!("Step {step}: retrieved {size} bytes");
    }
}

#[tokio::test]
async fn test_fdb_concurrent_list() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Arc::new(Fdb::open(Some(&config), None).expect("failed to create FDB"));

    // Archive test data
    for i in 0..4 {
        archive_test_data(&fdb, &i.to_string());
    }
    fdb.flush().expect("flush failed");

    let mut tasks = JoinSet::new();

    // Spawn multiple tasks that list data concurrently
    for _ in 0..4 {
        let fdb = Arc::clone(&fdb);

        tasks.spawn(async move {
            let request = Request::new()
                .with("class", "rd")
                .with("expver", "xxxx")
                .with("stream", "oper");

            let entries: Vec<_> = fdb
                .list(
                    &request,
                    ListOptions {
                        depth: 3,
                        deduplicate: false,
                    },
                )
                .expect("list failed")
                .collect();
            entries.len()
        });
    }

    let mut counts = Vec::new();
    while let Some(result) = tasks.join_next().await {
        counts.push(result.expect("task panicked"));
    }

    // All tasks should see the same number of entries
    assert!(counts.iter().all(|&c| c == counts[0]));
    println!("Concurrent list: all tasks found {} entries", counts[0]);
}

#[tokio::test]
async fn test_fdb_spawn_blocking_pattern() {
    // Test the recommended pattern for using FDB in async code:
    // use spawn_blocking for operations that may block
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());

    let fdb = Arc::new(Fdb::open(Some(&config), None).expect("failed to create FDB"));
    let grib_data =
        Arc::new(fs::read(fixtures_dir().join("synth11.grib")).expect("failed to read GRIB"));

    // Archive using spawn_blocking
    let fdb_clone = Arc::clone(&fdb);
    let grib_clone = Arc::clone(&grib_data);
    tokio::task::spawn_blocking(move || {
        let key = Key::new()
            .with("class", "rd")
            .with("expver", "xxxx")
            .with("stream", "oper")
            .with("date", "20230508")
            .with("time", "1200")
            .with("type", "fc")
            .with("levtype", "sfc")
            .with("step", "1")
            .with("param", "151130");

        fdb_clone
            .archive(&key, &grib_clone)
            .expect("archive failed");
        fdb_clone.flush().expect("flush failed");
    })
    .await
    .expect("spawn_blocking failed");

    // Retrieve using spawn_blocking
    let fdb_clone = Arc::clone(&fdb);
    let result = tokio::task::spawn_blocking(move || {
        let key = Key::new()
            .with("class", "rd")
            .with("expver", "xxxx")
            .with("stream", "oper")
            .with("date", "20230508")
            .with("time", "1200")
            .with("type", "fc")
            .with("levtype", "sfc")
            .with("step", "1")
            .with("param", "151130");

        let request = request_from_key(&key);
        let mut reader = fdb_clone.retrieve(&request).expect("retrieve failed");

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).expect("read failed");
        buf.len()
    })
    .await
    .expect("spawn_blocking failed");

    assert!(result > 0);
    println!("spawn_blocking pattern: retrieved {result} bytes");
}
