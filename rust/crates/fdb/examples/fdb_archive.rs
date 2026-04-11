//! Archive GRIB data to FDB.
//!
//! Run with: `cargo run --example fdb_archive -p fdb -- <config.yaml> <data.grib>`
//!
//! Or to archive using raw GRIB metadata extraction:
//! `cargo run --example fdb_archive -p fdb -- <config.yaml> <data.grib> --raw`

use std::path::Path;
use std::{env, fs};

use fdb::{Fdb, Key};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <config.yaml> <data.grib> [--raw]", args[0]);
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --raw    Archive using GRIB metadata extraction (no key needed)");
        std::process::exit(1);
    }

    let config_path = Path::new(&args[1]);
    let grib_path = &args[2];
    let use_raw = args.get(3).is_some_and(|a| a == "--raw");

    // Open the FDB. Passing a `Path` (rather than a `&str`) routes through
    // `fdb5::Config::make`, which loads YAML or JSON and expands `~fdb`/
    // `fdb_home` references — no need to slurp the file into a String first.
    let fdb = Fdb::open(Some(config_path), None)?;
    println!("FDB handle created: {}", fdb.name());

    // Read GRIB data
    let data = fs::read(grib_path)?;
    println!("Read {} bytes from {}", data.len(), grib_path);

    if use_raw {
        // Archive using raw GRIB data - FDB extracts metadata from GRIB headers
        println!("Archiving using raw GRIB metadata...");
        fdb.archive_raw(&data)?;
    } else {
        // Archive with explicit key - metadata must match your FDB schema
        let key = Key::new()
            .with("class", "rd")
            .with("expver", "xxxx")
            .with("stream", "oper")
            .with("date", "20230508")
            .with("time", "1200")
            .with("type", "fc")
            .with("levtype", "sfc")
            .with("step", "0")
            .with("param", "151130");

        println!("Archiving with explicit key...");
        fdb.archive(&key, &data)?;
    }

    // Flush to persist
    let () = fdb.flush()?;
    println!("Data archived and flushed successfully");

    // Show stats
    let stats = fdb.stats();
    println!(
        "Stats: {} archives, {} flushes",
        stats.num_archive, stats.num_flush
    );

    Ok(())
}
