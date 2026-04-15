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
script stamps an RPATH onto the final binary so the dynamic linker can
find the FDB / eckit / metkit / eccodes libraries at runtime:

- **Vendored** (default): binary-relative entries (`@executable_path/fdb_libs`
  and `@executable_path/eccodes_libs` on macOS; `$ORIGIN/fdb_libs` and
  `$ORIGIN/eccodes_libs` on Linux). The vendored build copies the
  libraries into those subdirectories next to the compiled binary.
- **System** (`--features system`): absolute entries pointing at the
  `lib` directory that `find_package` resolved for each dependency.

### Distributing Portable Binaries

For a redistributable vendored build, copy these directories alongside
your binary:

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

The binary-relative RPATH means users can drop this tree anywhere on
disk and the binary keeps loading the libraries from alongside itself
— no wrapper script and no environment variables needed on either
platform.

## License

Apache-2.0
