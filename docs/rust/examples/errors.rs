use fdb::{Error, Fdb, UserConfig};

fn main() {
    eckit::init();

    // [errors]
    match Fdb::open(None, Some(UserConfig::default())) {
        Ok(_) => unreachable!(),
        Err(Error::Eckit(eckit::Error::UserError(msg))) => println!("{msg}"),
        Err(Error::Io(io)) => println!("{io}"),
        Err(other) => println!("{other}"),
    }
    // [end errors]
}
