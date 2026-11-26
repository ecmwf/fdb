//! `fdb-write`-style example: archive one or more GRIB files into FDB,
//! streaming each file through `Fdb::archive_reader` so the bytes are
//! never fully buffered in Rust before crossing the FFI boundary.
//!
//! Mirrors a sensible subset of the upstream `fdb-write` tool. The
//! upstream filter / modifier / multi-archiver knobs are intentionally
//! omitted — they are `MessageArchiver` features the Rust crate does
//! not expose. The streaming archive path itself is the part the
//! reviewer redesign (note 7+24) was meant to enable.
//!
//! # Examples
//!
//! ```text
//! cargo run --example fdb_write -p fdb -- data.grib
//! cargo run --example fdb_write -p fdb -- --verbose data1.grib data2.grib
//! ```

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use fdb::Fdb;

/// `fdb-write`-style archiving tool. Reimplements a sensible subset of
/// the upstream `fdb-write` CLI on top of the Rust `fdb` binding.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// One or more GRIB files to archive.
    #[arg(required = true)]
    paths: Vec<PathBuf>,

    /// Print each file as it is archived.
    #[arg(short, long)]
    verbose: bool,
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let fdb = Fdb::open_default()?;

    for path in &args.paths {
        if args.verbose {
            eprintln!("archiving {}", path.display());
        }

        // `BufReader` keeps the FFI callback round-trips reasonably
        // sized; without it the C++ side would call back into Rust for
        // every short read.
        let reader = BufReader::new(File::open(path)?);
        fdb.archive_reader(reader)?;
    }

    fdb.flush()?;
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
