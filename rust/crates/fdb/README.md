# fdb

Safe Rust wrapper for ECMWF's [FDB](https://github.com/ecmwf/fdb) (Fields DataBase).

The FDB is a domain-specific object store for meteorological data, developed at ECMWF for high-performance storage and retrieval of weather and climate data.

## Installation

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
fdb = "5.23"
```

The default `vendored` feature builds the FDB and its dependencies (eckit,
metkit, ecCodes) from source, which requires CMake, a C++17 compiler and
Python 3.

## Usage

Archive and retrieve work on a fully-specified key: every key the schema
requires before bottoming out at a datum must be set. A typical
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

## Cargo build features

- `vendored` (default) - Build the FDB and its dependencies (eckit, metkit,
  ecCodes) from source.
- `system` - Link against a system-installed FDB.

Lower-level feature flags (GRIB support, storage backends, experimental
features) live on the [`fdb-sys`](https://crates.io/crates/fdb-sys) crate;
see its README for the full list. The defaults inherited here enable GRIB,
the filesystem TOC backend, and remote FDB client support.

## Running

Binaries and `cargo run` need no `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH`
setup on macOS or Linux.

All C++ shared libraries use `@rpath` install names. Binary crates that
depend on `fdb` should add a one-line `build.rs`:

```rust
fn main() {
    bindman_utils::emit_rpaths();
}
```

This stamps absolute RPATH entries onto the final binary pointing at each
dependency's build output, so the dynamic linker finds them automatically.

### System / FHS-packaged installs (e.g. RPM, deb)

When the target system already provides FDB and its dependencies,
typically as distro packages installed under `/usr/lib{,64}` with headers
under `/usr/include`, you don't need the colocated layout. Build against the system libraries with:

```bash
cargo build --release --no-default-features --features system
```

The build script calls `find_package(fdb5)` (and the same for eckit /
metkit / eccodes), links the Rust binary against those system
libraries, and stamps absolute RPATH entries pointing at the lib
directories the CMake search resolved. A downstream package can then
install the binary to a standard location such as `/usr/bin` and rely
on the distro's own `libfdb5` / `libeckit` / `libmetkit` / `libeccodes`
packages for the shared libraries, with no directories to copy and no
environment variables to set.

Typical packaging setups:

- **RPM / deb**: depend on the distro's FDB `-devel` packages at build
  time, depend on the runtime packages at install time, and build with
  `--features system`. Binary goes to `/usr/bin`, libs stay where the
  distro packages put them.
- **Custom prefix**: point `CMAKE_PREFIX_PATH` at your install tree
  before running cargo (e.g.
  `CMAKE_PREFIX_PATH=/opt/ecmwf cargo build --features system`).
  Everything else is automatic.

## Copyright and License

Copyright 1996- European Centre for Medium-Range Weather Forecasts (ECMWF).

This software is licensed under the terms of the [Apache License, Version 2.0](LICENSE) which can also be obtained at http://www.apache.org/licenses/LICENSE-2.0.

In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
