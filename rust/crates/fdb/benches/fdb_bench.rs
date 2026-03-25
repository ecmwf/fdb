//! Benchmarks for the fdb crate.
//!
//! Run with: `cargo bench --package fdb`
//!
//! Note: These benchmarks require FDB libraries to be available.
//! Some benchmarks require FDB setup and will be skipped if setup fails.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use fdb::{Fdb, Key, Request};
use std::sync::OnceLock;

// FDB setup for benchmarks that need data
mod fdb_setup {
    use fdb::{Fdb, Key};
    use std::env;
    use std::fs;
    use std::path::PathBuf;

    pub struct TestFdb;

    fn project_root() -> PathBuf {
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(manifest_dir)
            .parent()
            .expect("parent dir")
            .parent()
            .expect("grandparent dir")
            .to_path_buf()
    }

    pub fn setup() -> Option<TestFdb> {
        let root = project_root();
        let fdb_dir = root.join("target/bench-fdb");
        let fixtures_dir = root.join("tests/fixtures");

        // Create fixed directory
        fs::create_dir_all(&fdb_dir).ok()?;

        // Copy schema if not exists
        let schema_src = fixtures_dir.join("schema");
        let schema_dst = fdb_dir.join("schema");
        if !schema_dst.exists() {
            fs::copy(&schema_src, &schema_dst).ok()?;
        }

        let config = format!(
            "---\ntype: local\nengine: toc\nschema: {}/schema\nspaces:\n  - roots:\n      - path: {}\n",
            fdb_dir.display(),
            fdb_dir.display()
        );

        // Save config for C++ benchmarks
        fs::write(fdb_dir.join("fdb5_config.yaml"), &config).ok()?;

        // Set FDB config
        unsafe {
            env::set_var("FDB5_CONFIG", &config);
        }

        let fdb = Fdb::from_yaml(&config).ok()?;

        // Read test GRIB data
        let grib_path = fixtures_dir.join("synth11.grib");
        let grib_data = fs::read(&grib_path).ok()?;

        // Archive with keys matching the test data
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

        fdb.archive(&key, &grib_data).ok()?;
        fdb.flush().ok()?;

        Some(TestFdb)
    }
}

static FDB_SETUP: OnceLock<Option<fdb_setup::TestFdb>> = OnceLock::new();

fn get_fdb_setup() -> Option<&'static fdb_setup::TestFdb> {
    FDB_SETUP.get_or_init(fdb_setup::setup).as_ref()
}

/// Benchmark FDB handle creation.
fn bench_handle_creation(c: &mut Criterion) {
    c.bench_function("fdb_handle_creation", |b| {
        b.iter(|| black_box(Fdb::new().expect("failed to create handle")));
    });
}

/// Benchmark version string retrieval.
fn bench_version(c: &mut Criterion) {
    c.bench_function("fdb_version", |b| b.iter(|| black_box(Fdb::version())));
}

/// Benchmark Key creation with builder pattern.
fn bench_key_creation(c: &mut Criterion) {
    c.bench_function("fdb_key_creation", |b| {
        b.iter(|| {
            black_box(
                Key::new()
                    .with("class", "rd")
                    .with("expver", "xxxx")
                    .with("stream", "oper")
                    .with("date", "20230508")
                    .with("time", "1200"),
            );
        });
    });
}

/// Benchmark Request creation with builder pattern.
fn bench_request_creation(c: &mut Criterion) {
    c.bench_function("fdb_request_creation", |b| {
        b.iter(|| {
            black_box(
                Request::new()
                    .with("class", "rd")
                    .with("expver", "xxxx")
                    .with("stream", "oper")
                    .with("date", "20230508")
                    .with("time", "1200"),
            );
        });
    });
}

/// Benchmark Request creation with multiple values.
fn bench_request_multi_values(c: &mut Criterion) {
    c.bench_function("fdb_request_multi_values", |b| {
        b.iter(|| {
            black_box(
                Request::new()
                    .with("class", "rd")
                    .with_values("step", &["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]),
            );
        });
    });
}

/// Benchmark list operation (requires FDB setup).
fn bench_list(c: &mut Criterion) {
    let Some(_fdb) = get_fdb_setup() else {
        eprintln!("Skipping list benchmark: FDB setup failed");
        return;
    };

    let fdb = Fdb::new().expect("failed to create FDB handle");
    let request = Request::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper");

    c.bench_function("fdb_list", |b| {
        b.iter(|| {
            let results: Vec<_> = fdb.list(&request, 3, false).expect("list failed").collect();
            black_box(results);
        });
    });
}

/// Benchmark axes query (requires FDB setup).
fn bench_axes(c: &mut Criterion) {
    let Some(_fdb) = get_fdb_setup() else {
        eprintln!("Skipping axes benchmark: FDB setup failed");
        return;
    };

    let fdb = Fdb::new().expect("failed to create FDB handle");
    let request = Request::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper");

    c.bench_function("fdb_axes", |b| {
        b.iter(|| {
            let axes = fdb.axes(&request, 3).expect("axes query failed");
            black_box(axes);
        });
    });
}

/// Benchmark id/name/stats (read-only operations).
fn bench_readonly_ops(c: &mut Criterion) {
    let fdb = Fdb::new().expect("failed to create FDB handle");

    c.bench_function("fdb_id", |b| b.iter(|| black_box(fdb.id())));

    c.bench_function("fdb_name", |b| b.iter(|| black_box(fdb.name())));

    c.bench_function("fdb_stats", |b| b.iter(|| black_box(fdb.stats())));
}

criterion_group!(
    benches,
    bench_handle_creation,
    bench_version,
    bench_key_creation,
    bench_request_creation,
    bench_request_multi_values,
    bench_list,
    bench_axes,
    bench_readonly_ops,
);

criterion_main!(benches);
