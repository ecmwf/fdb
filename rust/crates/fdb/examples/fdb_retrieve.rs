//! Retrieve data from FDB.
//!
//! Run with: `cargo run --example fdb_retrieve -p fdb -- <request> [output.grib]`
//!
//! Examples:
//!   cargo run --example `fdb_retrieve` -p fdb -- "retrieve, class=rd,expver=xxxx,..."
//!   cargo run --example `fdb_retrieve` -p fdb -- "retrieve, class=rd,..." output.grib

use std::env;
use std::fs::File;
use std::io::{Read, Write};

use fdb::Fdb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <request> [output.grib]", args[0]);
        eprintln!();
        eprintln!("Request format: retrieve, key=value, key=value, ...");
        eprintln!(
            "Example: retrieve, class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=0,param=151130"
        );
        std::process::exit(1);
    }

    eckit::init();

    let fdb = Fdb::open_default()?;
    let parsed = metkit::parse(&args[1], false)?;
    let request = parsed.at(0)?;

    println!("Retrieving data...");
    let mut handle = fdb.retrieve(&request)?;
    handle.open_for_read()?;

    let mut buffer = Vec::new();
    let bytes_read = handle.read_to_end(&mut buffer)?;
    println!("Retrieved {bytes_read} bytes");

    // Write to file or show summary
    if let Some(output_path) = args.get(2) {
        let mut file = File::create(output_path)?;
        file.write_all(&buffer)?;
        println!("Written to {output_path}");
    } else {
        // Show first few bytes as hex
        let preview: Vec<String> = buffer.iter().take(32).map(|b| format!("{b:02x}")).collect();
        println!("Data preview: {}", preview.join(" "));
        if buffer.len() > 32 {
            println!("... ({} more bytes)", buffer.len() - 32);
        }
    }

    Ok(())
}
