use std::path::Path;

use fdb::{Fdb, UserConfig};

// [open]
fn open_from_environment() -> fdb::Result<Fdb> {
    Fdb::open_default()
}

fn open_from_file(path: &Path) -> fdb::Result<Fdb> {
    let config = eckit::Config::from_path(path)?;
    Fdb::open(Some(&config), None)
}

fn open_with_overrides(yaml: &str) -> fdb::Result<Fdb> {
    let config: eckit::Config = yaml.parse()?;
    let user = UserConfig {
        use_sub_toc: true,
        ..UserConfig::default()
    };
    Fdb::open(Some(&config), Some(user))
}
// [end open]

fn main() -> fdb::Result<()> {
    eckit::init();
    let fdb = match std::env::args().nth(1) {
        Some(path) => open_from_file(Path::new(&path))?,
        None => open_from_environment()?,
    };
    println!("{} {}", fdb.name(), fdb::version());

    if let Ok(yaml) = std::env::var("FDB_CONFIG") {
        let fdb = open_with_overrides(&yaml)?;
        println!("{}", fdb.name());
    }
    Ok(())
}
