//! C++ exceptions reach Rust as the matching `Error` variant.

use fdb::{Error, Fdb, Key};

#[test]
fn unknown_fdb_type_is_serious_bug() {
    eckit::init();
    let config: eckit::Config = "type: nonsense\n".parse().expect("yaml");
    let Err(err) = Fdb::open(Some(&config), None) else {
        panic!("opened an FDB of unknown type");
    };
    assert!(
        matches!(err, Error::Eckit(eckit::Error::SeriousBug(_))),
        "got {err:?}"
    );
}

#[test]
fn broken_schema_is_schema_error() {
    eckit::init();
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("schema"), "[ ? ]\n").expect("schema");
    let yaml = format!(
        "type: local\nengine: toc\nschema: {root}/schema\nspaces:\n  - roots:\n      - path: {root}\n",
        root = dir.path().display()
    );
    let config: eckit::Config = yaml.parse().expect("yaml");

    let key = Key::new().with("class", "rd").with("expver", "xxxx");
    let Err(err) = Fdb::open(Some(&config), None).and_then(|fdb| fdb.archive(&key, b"GRIB")) else {
        panic!("archived against a schema that does not parse");
    };
    assert!(matches!(err, Error::SchemaError(_)), "got {err:?}");
}
