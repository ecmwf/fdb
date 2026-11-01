# FDB Rust bindings

Rust bindings for ECMWF's [FDB](https://github.com/ecmwf/fdb) (Fields DataBase).

## Overview

The FDB is a domain-specific object store for meteorological data, developed
at ECMWF for high-performance storage and retrieval of weather and climate
data. These crates make it usable from Rust: the C++ library is built from
source (or located on the system) at build time, and its archive, retrieve,
list and administrative operations are exposed through a safe API.

## Installation

Add the safe wrapper to your `Cargo.toml`:

```toml
[dependencies]
fdb = "5.25"
```

Building requires CMake, a C++17 compiler and Python 3. To link against an
existing FDB installation instead of building from source, use the `system`
feature:

```toml
[dependencies]
fdb = { version = "5.25", default-features = false, features = ["system"] }
```

## Crates

- **fdb**: the safe API. Archive, retrieve, list and the administrative
  iterators, with FDB and eckit exceptions surfaced as typed Rust errors.
- **fdb-sys**: the low-level FFI layer using [CXX](https://cxx.rs/). Builds
  the FDB C++ library from source (`vendored`) or links against a system
  installation (`system`), and exposes the raw bridge. Its Cargo features
  control which parts of the C++ library are compiled.

The workspace also contains **fdb-hammer**, a benchmark and stress test tool
that is not published.

## Copyright and License

Copyright 1996- European Centre for Medium-Range Weather Forecasts (ECMWF).

This software is licensed under the terms of the [Apache License, Version 2.0](LICENSE) which can also be obtained at http://www.apache.org/licenses/LICENSE-2.0.

In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
