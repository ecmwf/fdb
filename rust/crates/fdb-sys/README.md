# fdb-sys

Low-level Rust bindings to ECMWF's [FDB](https://github.com/ecmwf/fdb) (Fields DataBase) C++ library.

This crate provides raw FFI bindings using [cxx](https://cxx.rs/). For a safe, ergonomic API, use the [`fdb`](https://crates.io/crates/fdb) crate instead.

## Features

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

### Storage backends (off by default; require external libraries)

- `radosfdb` - Ceph/RADOS object store backend (requires RADOS).
- `lustre` - Lustre file striping control (requires LUSTREAPI).
- `daosfdb` - DAOS object store backend (requires DAOS).
- `daos-admin` - DAOS pool management (requires DAOS).
- `dummy-daos` - Filesystem-emulated DAOS (no DAOS install needed).

### Other (off by default)

- `experimental` - Experimental upstream features.
- `sandbox` - Sandbox builds.

## License

Apache-2.0
