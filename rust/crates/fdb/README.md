# fdb

Safe Rust wrapper for ECMWF's [FDB5](https://github.com/ecmwf/fdb) (Fields DataBase).

FDB is a domain-specific object store for meteorological data, developed at ECMWF for high-performance storage and retrieval of weather and climate data.

## Usage

```rust
use fdb::{FDB, Key, WriteRequest};

// Open FDB with default configuration
let fdb = FDB::open()?;

// Write data
let key = Key::new()
    .set("class", "od")
    .set("stream", "oper")
    .set("type", "fc");
let request = WriteRequest::new(&key);
fdb.archive(&request, &data)?;

// Read data back
let results = fdb.retrieve(&request)?;
```

## Features

- `vendored` (default) - Build FDB5 and dependencies from source
- `system` - Link against system-installed FDB5

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
├── eccodes_libs/        # eccodes, libaec libraries
└── eccodes_resources/   # GRIB/BUFR definitions (if using eccodes)
    ├── definitions/
    └── samples/
```

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
