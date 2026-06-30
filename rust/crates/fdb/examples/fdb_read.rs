//! `fdb-read`-style example: retrieve FDB data matching a MARS request
//! and stream it to a target file (or stdout).
//!
//! Mirrors a sensible subset of the upstream `fdb-read` tool. The
//! upstream `--extract` (build a request from a GRIB file) and
//! `--statistics` flags are intentionally omitted — they require
//! bindings (`MessageDecoder`, the timing collector) that the Rust
//! crate does not expose.
//!
//! # Examples
//!
//! ```text
//! cargo run --example fdb_read -p fdb -- class=od,expver=0001 out.grib
//! cargo run --example fdb_read -p fdb -- class=rd,expver=xxxx -
//! ```
//!
//! Use `-` as the target to write to stdout (handy for piping into
//! `grib_dump`, `cat`, etc.).

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use fdb::{Fdb, Request};

/// `fdb-read`-style retrieval tool. Reimplements a sensible subset of
/// the upstream `fdb-read` CLI on top of the Rust `fdb` binding.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// MARS request, e.g. `class=od,expver=0001,date=20230508`.
    request: String,

    /// Target path. Use `-` to write to stdout.
    target: PathBuf,
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let request: Request = args.request.parse()?;
    let fdb = Fdb::open_default()?;

    // `retrieve` hands back a `DataReader` (which implements
    // `std::io::Read`) — exactly the streaming retrieval path the
    // reviewer redesign was meant to enable.
    let mut reader = fdb.retrieve(&request)?;

    // Open the target. `-` means stdout, matching the convention of
    // `fdb-read`'s sibling tools and most Unix utilities.
    let bytes_copied = if args.target == Path::new("-") {
        let stdout = io::stdout();
        let mut out = stdout.lock();
        io::copy(&mut reader, &mut out)?
    } else {
        let file = File::create(&args.target)?;
        let mut out = BufWriter::new(file);
        let n = io::copy(&mut reader, &mut out)?;
        out.flush()?;
        n
    };

    eprintln!("retrieved {bytes_copied} bytes");
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
