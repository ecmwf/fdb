//! `fdb-list`-style example: list FDB entries matching a MARS request.
//!
//! Mirrors a sensible subset of the upstream `fdb-list` tool. Demonstrates
//! that the public Rust binding is complete enough to write tools against.
//!
//! # Examples
//!
//! ```text
//! cargo run --example fdb_list -p fdb -- class=od
//! cargo run --example fdb_list -p fdb -- --location --length class=rd,expver=xxxx
//! cargo run --example fdb_list -p fdb -- --depth 1 class=od
//! cargo run --example fdb_list -p fdb -- --compact class=rd,expver=xxxx
//! ```

use std::fmt::Write as _;
use std::io::{self, Write as _};
use std::process::ExitCode;

use clap::Parser;
use fdb::{Fdb, ListElement, ListOptions, Request};

/// `fdb-list`-style listing tool. Reimplements a sensible subset of the
/// upstream `fdb-list` CLI on top of the Rust `fdb` binding.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
// CLI flag bag — six bools is normal for a tool like this; the clippy lint
// applies to "real" types where booleans usually want a state enum.
#[allow(clippy::struct_excessive_bools)]
struct Args {
    /// MARS request, e.g. `class=od,expver=0001`.
    request: String,

    /// Also print the location of each field.
    #[arg(long)]
    location: bool,

    /// Also print the field size.
    #[arg(long)]
    length: bool,

    /// Also print the index timestamp.
    #[arg(long)]
    timestamp: bool,

    /// Output entries up to N levels deep [1-3].
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(i32).range(1..=3))]
    depth: i32,

    /// Include masked / duplicate entries (no deduplication).
    #[arg(long)]
    full: bool,

    /// Aggregate the results into compact MARS-request summaries,
    /// mirroring `fdb-list --compact`. Incompatible with `--location`,
    /// `--length`, `--timestamp`, and `--full`.
    #[arg(long, conflicts_with_all = ["location", "length", "timestamp", "full"])]
    compact: bool,

    /// Streamlined output (no leading status line or trailing summary).
    #[arg(long)]
    porcelain: bool,
}

/// Format one `ListElement` mirroring upstream `fdb-list`'s output:
/// `{db_key}{index_key}{datum_key}[, location][, length=N][, timestamp=N]`
fn format_item(item: &ListElement, args: &Args) -> Result<String, std::fmt::Error> {
    fn write_part(out: &mut String, entries: &[(String, String)]) -> std::fmt::Result {
        out.push('{');
        let mut first = true;
        for (k, v) in entries {
            if !first {
                out.push(',');
            }
            first = false;
            write!(out, "{k}={v}")?;
        }
        out.push('}');
        Ok(())
    }

    let mut out = String::new();
    write_part(&mut out, &item.db_key)?;
    if !item.index_key.is_empty() {
        write_part(&mut out, &item.index_key)?;
        if !item.datum_key.is_empty() {
            write_part(&mut out, &item.datum_key)?;
            if args.location {
                out.push_str(", ");
                out.push_str(&item.uri);
            }
        }
    }
    if args.length {
        write!(out, ", length={}", item.length)?;
    }
    if args.timestamp {
        write!(out, ", timestamp={}", item.timestamp)?;
    }
    Ok(out)
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let request: Request = args.request.parse()?;
    let fdb = Fdb::open_default()?;

    if !args.porcelain {
        println!("Listing for request:");
        println!("  {}", args.request);
        println!();
    }

    // `fdb-list` deduplicates by default; `--full` opts in to seeing the
    // masked entries too. `ListOptions` takes a `deduplicate` flag, so pass
    // the negation.
    let options = ListOptions {
        depth: args.depth,
        deduplicate: !args.full,
    };
    let list_iter = fdb.list(&request, options)?;

    if args.compact {
        let stdout = io::stdout();
        let mut out = stdout.lock();
        let summary = list_iter.dump_compact(&mut out)?;
        out.flush()?;
        if !args.porcelain {
            println!();
            println!("Entries : {}", summary.fields);
            println!("Total   : {} bytes", summary.total_bytes);
        }
        return Ok(());
    }

    let mut count = 0;
    for item in list_iter {
        let item = item?;
        println!("{}", format_item(&item, args)?);
        count += 1;
    }

    if !args.porcelain {
        println!();
        println!("{count} field(s) matched");
    }

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
