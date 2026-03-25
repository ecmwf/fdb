//! Retrieve data from FDB.
//!
//! Run with: `cargo run --example fdb_retrieve -p fdb -- <request> [output.grib]`
//!
//! Examples:
//!   cargo run --example `fdb_retrieve` -p fdb -- class=rd,expver=xxxx,date=20230508,...
//!   cargo run --example `fdb_retrieve` -p fdb -- class=rd,expver=xxxx,... output.grib

use std::env;
use std::fs::File;
use std::io::{Read, Write};

use fdb::{Fdb, Request};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <request> [output.grib]", args[0]);
        eprintln!();
        eprintln!("Request format: key=value,key=value,...");
        eprintln!(
            "Example: class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=0,param=151130"
        );
        std::process::exit(1);
    }

    let fdb = Fdb::new()?;
    let request: Request = args[1].parse()?;

    println!("Retrieving data...");
    let mut reader = fdb.retrieve(&request)?;

    let mut buffer = Vec::new();
    let bytes_read = reader.read_to_end(&mut buffer)?;
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
