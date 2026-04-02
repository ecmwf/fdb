//! Query available axes (dimensions) in FDB.
//!
//! Run with: `cargo run --example fdb_axes -p fdb -- [key=value,key=value,...]`
//!
//! Examples:
//!
//! ```text
//! cargo run --example fdb_axes -p fdb -- class=od
//! cargo run --example fdb_axes -p fdb -- class=rd,expver=xxxx
//! ```

use std::env;

use fdb::{Fdb, Request};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let fdb = Fdb::new()?;
    println!("FDB: {}", fdb.name());

    let request: Request = if args.len() > 1 {
        args[1].parse()?
    } else {
        println!("Usage: {} [key=value,key=value,...]", args[0]);
        println!("Using default: class=od");
        Request::new().with("class", "od")
    };

    println!("Querying axes...\n");

    // Query axes with depth=3 (full traversal)
    let axes = fdb.axes(&request, 3)?;

    if axes.is_empty() {
        println!("No axes found for the given request.");
    } else {
        for (name, values) in &axes {
            println!("{name}:");
            for value in values {
                println!("  - {value}");
            }
        }
        println!("\nFound {} axis/axes", axes.len());
    }

    Ok(())
}
