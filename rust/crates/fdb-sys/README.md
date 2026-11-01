# fdb-sys

Low-level Rust bindings to ECMWF's [FDB](https://github.com/ecmwf/fdb) (Fields DataBase) C++ library.

This crate provides raw FFI bindings using [cxx](https://cxx.rs/). For a safe API, use the [`fdb`](https://crates.io/crates/fdb) crate instead.
The design of both crates is described in the Rust interface section of the FDB
documentation (`docs/rust` in the repository).

## Cargo build features

These flags control what the underlying C++ FDB library is compiled with.

### Build strategy (mutually exclusive)

- `vendored` - Build the FDB and its dependencies (eckit, metkit, ecCodes) from source.
- `system` - Link against system-installed FDB.

Note: neither is enabled by default on `fdb-sys` itself. End users should
depend on the higher-level [`fdb`](https://crates.io/crates/fdb) crate, which
defaults to `vendored`. If you depend on `fdb-sys` directly you must select
one explicitly.

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
