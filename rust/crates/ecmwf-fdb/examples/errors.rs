use fdb::{Error, Fdb};

fn main() -> fdb::Result<()> {
    eckit::init();
    let config: eckit::Config = "type: nonsense\n".parse()?;

    // [errors]
    match Fdb::open(Some(&config), None) {
        Ok(_) => println!("opened"),
        Err(Error::Eckit(eckit::Error::SeriousBug(msg))) => println!("{msg}"),
        Err(Error::Io(io)) => println!("{io}"),
        Err(other) => println!("{other}"),
    }
    // [end errors]
    Ok(())
}
