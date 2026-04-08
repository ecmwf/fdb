[![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/emerging_badge.svg)](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity#emerging)

> \[!IMPORTANT\]
> This software is **Emerging** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).

# PyFDB

`PyFDB` is the Python interface to the `FDB <github.com/ecmwf/fdb>`__, a
domain‑specific object store designed to efficiently archive, index, list, and
retrieve GRIB fields produced by numerical weather prediction workflows. It
provides a thin, idiomatic Python layer over the `FDB` client library installed
on your system, so you can drive FDB operations directly from Python scripts
and notebooks. 

The `FDB <github.com/ecmwf/fdb>`__ itself is part of `ECMWF`’s
high‑performance data infrastructure: it stores each GRIB message as a field,
indexes it by meteorological metadata (e.g., `parameter`, `level`, `date/time`),
and serves recent outputs to post‑processing tasks and users. In operational
use, FDB acts as a hot cache in front of the long‑term MARS archive, enabling
fast access to newly generated data. 

If you’re new to `FDB`, you may want to skim the `FDB` documentation for concepts
(keys, requests, schema, spaces) and the overall architecture before
proceeding. 

## Documentation

For implementation details and tooling, see the `FDB project pages <https://fields-database.readthedocs.io/en/latest/index.html>`__.

## Installation via PyPI

Install the package from PyPI in your `venv`:

```
   uv venv
   source .venv/bin/activate
   uv pip install pyfdb
```

This will bring in some necessary binary dependencies for you.
Set the `FDB_HOME` environment variable accordingly:

```
    export FDB_HOME=<path_to_fdb_home>
```

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ecmwf/fdb/blob/develop/LICENSE)
