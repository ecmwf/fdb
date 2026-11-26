# ecmwf-fdb-sys

Low-level Rust bindings to ECMWF's [FDB](https://github.com/ecmwf/fdb) (Fields DataBase) C++ library.

This crate provides raw FFI bindings using [cxx](https://cxx.rs/). For a safe API, use the [`ecmwf-fdb`](https://crates.io/crates/ecmwf-fdb) crate instead.

## Cargo build features

These flags control what the underlying C++ FDB library is compiled with.

### Build strategy (mutually exclusive)

- `vendored` (default) - Build the FDB and its dependencies (eckit, metkit, ecCodes) from source.
- `system` - Link against system-installed FDB. Disable the default features to use it.

End users should depend on the higher-level [`ecmwf-fdb`](https://crates.io/crates/ecmwf-fdb)
crate rather than on `ecmwf-fdb-sys` directly.

### Core (enabled by default)

- `grib` - GRIB format support. Pulls in `eccodes-sys/product-grib` and
  `metkit-sys/grib` so the GRIB message splitter is registered with
  `eckit::message::Splitter`.
- `tocfdb` - Filesystem TOC backend (the standard local FDB store).
- `fdb-remote` - Client support for remote FDB servers.
- `memfs` - Bake the eccodes definition/sample tables into `libeccodes`
  itself (`ENABLE_MEMFS=ON`). With this on, end users do **not** have to
  ship an `eccodes_resources/` directory next to their binary. Build-time
  requirement: Python 3 must be on `PATH` (the upstream CMake step uses a
  Python helper to embed the resource files).

### Storage backends (off by default; require external libraries)

- `radosfdb` - Ceph/RADOS object store backend (requires RADOS).
- `lustre` - Lustre file striping control (requires LUSTREAPI).
- `daosfdb` - DAOS object store backend (requires DAOS).
- `daos-admin` - DAOS pool management (requires DAOS).
- `dummy-daos` - Filesystem-emulated DAOS (no DAOS install needed).

### Other (off by default)

- `experimental` - Experimental upstream features.
- `sandbox` - Sandbox builds.

## Copyright and License

Copyright 1996- European Centre for Medium-Range Weather Forecasts (ECMWF).

This software is licensed under the terms of the [Apache License, Version 2.0](LICENSE) which can also be obtained at http://www.apache.org/licenses/LICENSE-2.0.

In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
