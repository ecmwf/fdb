//! Query available axes (dimensions) in FDB.
//!
//! # Examples
//!
//! ```text
//! cargo run --example fdb_axes -p fdb -- class=od,expver=0001
//! cargo run --example fdb_axes -p fdb -- class=rd,expver=xxxx
//! ```

use std::process::ExitCode;

use clap::Parser;
use fdb::{Fdb, Request};

/// Query the available axes (metadata dimensions) for a MARS request.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// MARS request selecting which databases to query,
    /// e.g. `class=rd,expver=xxxx`.
    request: String,
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let request: Request = args.request.parse()?;
    let fdb = Fdb::open_default()?;

    // Full traversal (db + index + datum) mirrors the behaviour of
    // `fdb-axes --depth 3` and is what most callers actually want.
    let axes = fdb.axes(&request, 3)?;

    if axes.is_empty() {
        println!("No data matches the given request.");
        return Ok(());
    }

    let mut total_values = 0usize;
    for (name, values) in &axes {
        println!("{name}:");
        for value in values {
            println!("  - {value}");
        }
        total_values += values.len();
    }
    println!(
        "\n{keys} key(s) covering {values} value(s)",
        keys = axes.len(),
        values = total_values,
    );

    Ok(())
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
