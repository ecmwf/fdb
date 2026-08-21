.. SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
.. SPDX-License-Identifier: Apache-2.0

.. _z3fdb_extractor_backends:

GRIB and GribJump Extractors
============================

Z3FDB ships two extractor backends. Both implement the same interface and both read the same
data from the same FDB; the only difference is how the values leave a GRIB message. This page
covers what each one does, how to choose, and the constraints each imposes on a view.

For the machinery around them, see :doc:`extractor`. That covers ownership, the factory, and
when the layout is established.

.. contents:: On this page
   :local:
   :depth: 1

Choosing between them
---------------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * -
     - ``ExtractorType.Grib``
     - ``ExtractorType.GribJump``
   * - How values are read
     - Full eccodes decode of the message
     - Jumps to the values inside the message
   * - Grid-point axis
     - Always one chunk: the whole field
     - Optionally split, via ``field_chunking``
   * - Best for
     - Reading whole fields
     - Reading a sub-range of each field, repeatedly
   * - Availability
     - Always
     - Opt-in build feature (see `Availability`_)

The rule of thumb: if you read whole fields, ``Grib`` is the simpler choice and there is nothing
to gain from ``GribJump``. If you slice into the grid-point axis, ``GribJump`` avoids decoding the values you are
going to discard. A time series at one location, or a region out of a global field, are the
typical cases, and ``field_chunking`` is what lets zarr fetch only the piece you asked for.

:ref:`tutorial_custom_store_mixed_extractors` builds one store containing both, which is a
reasonable pattern when different consumers of the same data have different access patterns.

GribExtractor
-------------

Reads GRIB messages from FDB and decodes them to ``float32`` via eccodes. The entire field is
decoded on every chunk access, so the implicit grid-point dimension is always a single chunk
covering the whole field.

Where a message carries a bitmap, the masked grid points are replaced with the view's fill
value (see :meth:`~z3fdb.SimpleStoreBuilder.fill_missing_value`).

GribJumpExtractor
-----------------

Uses the GribJump library to read grid-point values without performing a full GRIB decode.

Field enumeration still goes through FDB: the extractor inspects FDB to learn which fields
match the part's sub-request and in what order, then asks GribJump for the values. The order
matters, because it is what maps each field onto its slot in the chunk buffer.

**Field chunking.** ``field_chunking`` subdivides the implicit grid-point dimension into equally
sized zarr chunks::

   # 5248 grid points, split into 4 chunks of 1312
   ExtractorType.GribJump(field_chunking=Chunking.FixedSizeChunk(chunk_shape=1312))

Each ``extractInto`` call derives the range it needs from the chunk it was handed; nothing is
cached between calls, and there is no separate sub-range selection.

**Missing values.** GribJump returns a bitmask alongside the values, in which a *set* bit means
the point is valid and a clear bit means it is missing. Masked points are written as the view's
fill value.

Configuration
-------------

``fdb_config`` (both backends)
   Path to an FDB configuration YAML. When left as ``None``, the extractor inherits the path
   given to the store builder; if that is also unset, FDB resolves its own configuration from
   ``FDB5_CONFIG`` / ``FDB_HOME``.

``gribjump_config`` (GribJump only)
   Path to a GribJump configuration YAML. When set, the binding exports it as
   ``GRIBJUMP_CONFIG_FILE`` before constructing the GribJump object; when unset, that
   environment variable is used as-is.

``field_chunking`` (GribJump only)
   Chunking of the implicit grid-point dimension. Defaults to a single chunk covering the whole
   field. That is the only setting which can be mixed with a ``Grib`` part, see below.

An extractor configuration object is *copied* when it is handed to ``add_part``, so one object
can be passed to as many parts and as many builders as you like, and the ``fdb_config`` a
builder fills in never leaks back into your object.

Constraints
-----------

Three rules are enforced when the view is built. All three surface as ``RuntimeError`` in
Python, carrying the message quoted below.

**A field chunk size must divide the grid exactly.** The grid-point dimension is the one
dimension a zarr array cannot leave ragged, so 5248 points can be split into chunks of 1312 or
2624, but not 1000:

.. code-block:: text

   GribJumpExtractor: field chunk size 1000 does not evenly divide the window size 5248.

A size of zero is rejected earlier still, by ``Chunking.FixedSizeChunk`` itself.

**Every part of a view must cover the same grid.** The grid-point dimension is never the
extension axis. Like every other non-extension axis, all parts have to agree on it:

.. code-block:: text

   ChunkedDataViewBuilder::build: part 1 has 2048 grid points but part 0 has 5248. The
   grid-point dimension is never the extension axis, so every part must cover the same grid;
   a view cannot have a ragged last dimension.

**A GribJump part mixed with a Grib part must use whole-axis field chunking.** ``Grib`` always
returns the whole field, so its chunk on the grid-point axis is the full grid; a ``GribJump``
part using ``FixedSizeChunk`` would write a smaller block into a buffer laid out for a larger
one:

.. code-block:: text

   ChunkedDataViewBuilder::build: part 1 splits the grid-point dimension into chunks of 1312
   values but part 0 uses 5248. All parts must agree on the field chunking, so a GribJump part
   mixed with a Grib part has to use the default WholeAxisChunking.

Availability
------------

``GribExtractor`` is always present. ``GribJumpExtractor`` is built only when fdb is configured
with ``-DENABLE_ZARR_GRIBJUMP_EXTRACTOR=ON``, which is **off by default**. See
:ref:`z3fdb_installation`.

``ExtractorType.GribJump`` is importable and constructible in every build, so your code does not
have to branch on how the wheel was compiled. Only building a view from it fails:

.. code-block:: python

   from pychunked_data_view import has_gribjump_extractor

   if not has_gribjump_extractor:
       ...   # fall back to ExtractorType.Grib()

Concurrency
-----------

Each extractor serialises its own ``extractInto`` calls with a mutex: it drives a shared FDB
handle (and, for GribJump, a shared GribJump object) while the Python layer has released the
GIL, so concurrent chunk reads would otherwise race.

The unit of parallelism is therefore the *part*, not the chunk. Two threads reading chunks that
are served by the same part will take turns; two threads reading chunks served by different
parts proceed at the same time. Worth knowing when sizing a dask cluster against a
single-part store.

.. seealso::

   :doc:`extractor` for the ownership model and the ``ExtractorDefinition`` factory,
   :doc:`chunk_access` for where ``extractInto`` sits in the chunk-access pipeline, and
   :doc:`../api` for the full configuration reference.
