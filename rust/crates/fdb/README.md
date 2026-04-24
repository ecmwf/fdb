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
let fdb = Fdb::open_default()?;

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

Binaries and `cargo run` work out of the box on both macOS and Linux —
no `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` setup required. The build
script stamps RPATH entries onto the final binary so the dynamic linker
finds the libraries at runtime automatically.

### System / FHS-packaged installs (e.g. RPM, deb)

When the target system already provides FDB and its dependencies —
typically via separate distro packages installed under `/usr/lib{,64}`
— build against them with:

```bash
cargo build --release --no-default-features --features system
```

The build script calls `find_package(fdb5)` (and the same for eckit /
metkit / eccodes), links the Rust binary against those system
libraries, and stamps absolute RPATH entries pointing at the resolved
lib directories. Install the binary to `/usr/bin` (or any standard
location) and rely on the distro's own packages for the shared
libraries — no need to copy anything extra.

### Vendored / self-contained builds

With the default `vendored` feature the build compiles FDB and all its
dependencies from source and copies the resulting shared libraries next
to the binary. The RPATH is set to find them there, so the binary is
portable as-is.

The eccodes definition/sample tables are baked into `libeccodes` via
the default `memfs` feature, so there are no extra resource directories
to ship. (If you opt out of `memfs`, you also need to ship
`eccodes_resources/{definitions,samples}/` and point
`ECCODES_DEFINITION_PATH`/`ECCODES_SAMPLES_PATH` at them.)

## License

Apache-2.0
