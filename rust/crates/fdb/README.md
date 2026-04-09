# fdb

Safe Rust wrapper for ECMWF's [FDB](https://github.com/ecmwf/fdb) (Fields DataBase).

The FDB is a domain-specific object store for meteorological data, developed at ECMWF for high-performance storage and retrieval of weather and climate data.

## Usage

Archive and retrieve always work on a fully-specified key — every key the
schema requires before bottoming out at a datum must be set. A typical
schema (e.g. `class=od`, `stream=oper`) requires
`class, expver, stream, date, time, type, levtype, step, param` at minimum.

```rust,no_run
use fdb::{Fdb, Key, Request};
use std::io::Read;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
// Open the FDB. Picks up its configuration from the environment
// (`FDB_CONFIG_FILE` or similar); see the upstream FDB docs.
let fdb = Fdb::new()?;

let key = Key::new()
    .with("class", "od")
    .with("expver", "0001")
    .with("stream", "oper")
    .with("date", "20240101")
    .with("time", "0000")
    .with("type", "fc")
    .with("levtype", "sfc")
    .with("step", "0")
    .with("param", "151130");

let data: &[u8] = b"...field bytes...";
fdb.archive(&key, data)?;
fdb.flush()?;

// Retrieve uses the same fully-specified key (any unset key would match
// every value, which is rarely what you want).
let request = Request::new()
    .with("class", "od")
    .with("expver", "0001")
    .with("stream", "oper")
    .with("date", "20240101")
    .with("time", "0000")
    .with("type", "fc")
    .with("levtype", "sfc")
    .with("step", "0")
    .with("param", "151130");
let mut reader = fdb.retrieve(&request)?;
let mut results = Vec::new();
reader.read_to_end(&mut results)?;
# Ok(())
# }
```

## Features

- `vendored` (default) - Build the FDB and its dependencies (eckit, metkit,
  ecCodes) from source.
- `system` - Link against a system-installed FDB.

Lower-level feature flags (GRIB support, storage backends, experimental
features) live on the [`fdb-sys`](https://crates.io/crates/fdb-sys) crate;
see its README for the full list. The defaults inherited here enable GRIB,
the filesystem TOC backend, and remote FDB client support.

## Running

### macOS

Binaries work out of the box - no environment variables needed.

### Linux

Set library path before running:

```bash
export LD_LIBRARY_PATH=$PWD/target/release/fdb_libs:$PWD/target/release/eccodes_libs:$LD_LIBRARY_PATH
./target/release/my-fdb-app
```

### Distributing Portable Binaries

Copy these directories alongside your binary:

```
my_app/
├── my-fdb-app           # Your binary
├── fdb_libs/            # FDB, eckit, metkit libraries
└── eccodes_libs/        # eccodes, libaec libraries
```

The eccodes definition/sample tables are baked into `libeccodes` itself
via the default `memfs` feature, so there's no `eccodes_resources/`
directory to ship. (If you opt out of `memfs`, you'd also need to ship
`eccodes_resources/{definitions,samples}/` next to the binary and point
`ECCODES_DEFINITION_PATH`/`ECCODES_SAMPLES_PATH` at it.)

**macOS**: Works immediately after copying.

**Linux**: Create a wrapper script:

```bash
#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
export LD_LIBRARY_PATH="$DIR/fdb_libs:$DIR/eccodes_libs:$LD_LIBRARY_PATH"
exec "$DIR/my-fdb-app-bin" "$@"
```

## License

Apache-2.0
