//! C++ exceptions reach Rust as the matching `Error` variant.

use fdb::{Error, Fdb};

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
