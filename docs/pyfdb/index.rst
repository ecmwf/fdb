.. pyfdb documentation master file
    :author: ECMWF
    :date: 2025-11-10

.. _PyFDB_Introduction:

PyFDB
===================

:Version: |version|

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :hidden:

   installation
   examples
   api

`PyFDB` is the Python interface to ECMWF’s Fields DataBase (`FDB <github.com/ecmwf/fdb>`__), a
domain‑specific object store designed to efficiently archive, index, list, and
retrieve GRIB fields produced by numerical weather prediction workflows. It
provides a thin, idiomatic Python layer over the `FDB5` client library installed
on your system, so you can drive FDB operations directly from Python scripts
and notebooks. 

`FDB <github.com/ecmwf/fdb>`__ itself is part of `ECMWF`’s
high‑performance data infrastructure: it stores each GRIB message as a field,
indexes it by meteorological metadata (e.g., `parameter`, `level`, `date/time`),
and serves recent outputs to post‑processing tasks and users. In operational
use, FDB acts as a hot cache in front of the long‑term MARS archive, enabling
fast access to newly generated data. 

This documentation guides you through the use of the API by
showing examples of the following steps:

- Initialise an FDB client in Python, optionally with a custom configuration
- Archive GRIB messages into a `FDB`, 
- List what’s available via metadata queries
- Retrieve matching fields for downstream processing

Prerequisites to run the examples, ensure that FDB5 is installed and available
on your system and that required Python packages are present. 

If you’re new to FDB, you may want to skim the FDB documentation for concepts
(keys, requests, schema, spaces) and the overall architecture before
proceeding. 

For implementation details and tooling, see the `FDB project pages <https://fields-database.readthedocs.io/en/latest/index.html>`__.

