[![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/emerging_badge.svg)](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity#emerging)

> \[!IMPORTANT\]
> This software is **Emerging** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).

# Z3FDB

`Z3FDB` exposes an [FDB](https://github.com/ecmwf/fdb) archive as a read-only
[Zarr v3](https://zarr.dev) store - without copying your GRIB data. You
describe which data you want through MARS requests, map MARS keywords to Zarr
dimensions, and Z3FDB handles the rest: chunk accesses are translated into FDB
retrieves on the fly and the returned GRIB data is decoded to float32 in
memory.

If you're new to [FDB](https://github.com/ecmwf/fdb), skim its documentation
for concepts (keys, requests, schema, spaces) before proceeding.

## Installation via PyPI

Install the package together with the FDB Python client:

```
pip install z3fdb pyfdb
```

## Documentation

For implementation details, the full API reference, and chunking strategies,
see the [FDB project pages](https://sites.ecmwf.int/docs/fdb).

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ecmwf/fdb/blob/develop/LICENSE)
