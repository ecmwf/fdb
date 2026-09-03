.. SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
.. SPDX-License-Identifier: Apache-2.0

.. _Z3FDB_Introduction:

Z3FDB
=====

:Version: |version|

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :hidden:

   installation
   getting_started
   dimension_mapping
   architecture
   api

.. toctree::
   :maxdepth: 1
   :caption: Technical Insights:
   :hidden:

   technical_insights/dev_setup
   technical_insights/chunk_access
   technical_insights/buffer_layout
   technical_insights/extractor



Introduction
------------

Z3FDB enables FDB to function as a `Zarr <https://zarr.dev>`_ store without
copying your GRIB data. You describe which data you want through MARS requests,
map MARS keywords to Zarr dimensions, and Z3FDB handles the rest: chunk
requests are translated into FDB retrieves on the fly and the returned GRIB
data is decoded to float32 in memory.

.. admonition:: Supported data

   FDB can store arbitrary data, not just GRIB.
   At this point Z3FDB only supports data extraction from GRIB.
   It is the responsibility of the caller to ensure that only GRIB data is
   accessed through Z3FDB.

When to use Z3FDB
-----------------

Z3FDB is a good fit when a 2–5× slowdown compared to on-disk Zarr is
acceptable. That trade-off makes sense in two situations:

**Prototyping with an existing FDB**
   You have GRIB data in FDB and want a Zarr interface without first writing
   everything to disk. Redefining the store is a matter of changing a few lines
   of Python — no data copy required. Z3FDB stores are cheap to create because
   data is only fetched when a chunk is accessed.

**Very large datasets**
   You have more data in FDB than you could redundantly store as both GRIB
   and Zarr. Z3FDB gives you Zarr-compatible access with zero extra storage.

Installation
------------

Install Z3FDB from PyPI, see :doc:`installation` for the full instructions,
including how to run the test suite.

Next steps
----------

New to Z3FDB? Start with the step-by-step tutorial: :doc:`getting_started`.

For a full reference on how MARS keywords map to Zarr dimensions, chunking
strategies, fill values, and multi-part views, see :doc:`dimension_mapping`.
