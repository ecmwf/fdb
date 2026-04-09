//! Basic FDB example - shows version info and handle creation.
//!
//! Run with: `cargo run --example fdb_basic -p fdb`

use fdb::{ControlIdentifier, Fdb};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Print version info (works without FDB config)
    println!("FDB version: {}", fdb::version());
    println!("FDB git SHA1: {}", fdb::git_sha1());

    // Create a default handle (requires FDB_HOME or FDB5_CONFIG environment)
    let fdb = Fdb::open_default()?;
    println!("FDB handle created successfully");
    println!("FDB type: {}", fdb.name());
    println!("FDB id: {}", fdb.id());

    // Check capabilities
    println!("\nCapabilities:");
    println!(
        "  retrieve enabled: {}",
        fdb.enabled(ControlIdentifier::Retrieve)
    );
    println!(
        "  archive enabled: {}",
        fdb.enabled(ControlIdentifier::Archive)
    );
    println!("  list enabled: {}", fdb.enabled(ControlIdentifier::List));

    Ok(())
}
