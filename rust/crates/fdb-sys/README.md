# fdb-sys

Low-level Rust bindings to ECMWF's [FDB](https://github.com/ecmwf/fdb) (Fields DataBase) C++ library.

This crate provides raw FFI bindings using [cxx](https://cxx.rs/). For a safe, ergonomic API, use the [`fdb`](https://crates.io/crates/fdb) crate instead.

## Features

- `vendored` (default) - Build FDB5 and dependencies from source
- `system` - Link against system-installed FDB5
- `grib` - GRIB format support via ecCodes
- `tocfdb` - Filesystem TOC support
- `fdb-remote` - Remote FDB access

## License

Apache-2.0
