//! List fields in FDB matching a query.
//!
//! Run with: `cargo run --example fdb_list -p fdb -- [key=value,key=value,...]`
//!
//! Examples:
//!
//! ```text
//! cargo run --example fdb_list -p fdb -- class=od
//! cargo run --example fdb_list -p fdb -- class=rd,expver=xxxx
//! ```

use std::env;

use fdb::{Fdb, Request};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let fdb = Fdb::new()?;
    println!("FDB: {}", fdb.name());

    // Build request from command-line or use default
    let request: Request = if args.len() > 1 {
        args[1].parse()?
    } else {
        println!("Usage: {} [key=value,key=value,...]", args[0]);
        println!("Using default: class=od");
        Request::new().with("class", "od")
    };

    println!("Listing fields...\n");

    // List with depth=3 (full traversal), no deduplication
    let mut count = 0;
    for item in fdb.list(&request, 3, false)? {
        let item = item?;
        let key = item
            .full_key()
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        println!("  {{{key}}}");
        count += 1;
    }

    println!("\nFound {count} field(s)");
    Ok(())
}
