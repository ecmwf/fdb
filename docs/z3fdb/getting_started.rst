Getting Started
===============

This guide walks you through building your first Z3FDB store from scratch.
By the end you will have a working Zarr array backed by FDB GRIB data and
understand how to index into it.

Prerequisites
-------------

* Z3FDB installed — see :ref:`Z3FDB_Introduction` for build instructions.
* An FDB instance containing GRIB data accessible from your environment.
* ``zarr`` installed.

Your First Store
----------------

The example below creates a 3-dimensional Zarr array from two dates, four
time steps, and one surface parameter. Read it top to bottom — each step is
explained immediately after the code block.

.. code-block:: python

   import zarr
   from z3fdb import SimpleStoreBuilder, AxisDefinition, Chunking, ExtractorType

   builder = SimpleStoreBuilder()

   builder.add_part(
       {
           "class": "od",
           "domain": "g",
           "expver": "0001",
           "stream": "oper",
           "type": "fc",
           "date": ["2020-01-01", "2020-01-02"],   # 2 dates
           "time": ["0000", "0600", "1200", "1800"],  # 4 times
           "levtype": "sfc",
           "step": 0,
           "param": 167,
       },
       [
           AxisDefinition(["date"], Chunking.SINGLE_VALUE),  # → Dim 0, size 2
           AxisDefinition(["time"], Chunking.SINGLE_VALUE),  # → Dim 1, size 4
       ],
       ExtractorType.GRIB,
   )

   store = builder.build()
   data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

**What each piece does:**

``SimpleStoreBuilder``
   Collects MARS requests and axis definitions before assembling the store.
   Call :meth:`~z3fdb.SimpleStoreBuilder.add_part` once per MARS request,
   then :meth:`~z3fdb.SimpleStoreBuilder.build` to produce a Zarr store.

``add_part(mars_request, axes, ExtractorType.GRIB)``
   Registers one MARS request. The ``axes`` list controls how MARS keywords
   become Zarr dimensions. ``ExtractorType.GRIB`` tells Z3FDB the data is
   encoded as GRIB.

``AxisDefinition(keys, chunking)``
   Maps one or more MARS keywords to **exactly one** Zarr dimension.
   The position in the list sets the dimension index (0-based).

``Chunking.SINGLE_VALUE``
   The most common chunking mode: each value along the axis is its own chunk.
   Reading ``data[i, j, :]`` triggers exactly one FDB retrieve.
   See :doc:`dimension_mapping` for other options.

Understanding the Array Shape
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After ``build()``, ``data`` is a 3-dimensional array:

.. code-block:: text

   data.shape  ->  (2, 4, N)

   Dim 0 — date:        2 entries  (2020-01-01, 2020-01-02)
   Dim 1 — time:        4 entries  (0000, 0600, 1200, 1800)
   Dim 2 — grid points: N float32 values decoded from the GRIB field  [implicit]

The **implicit final dimension** always holds the decoded grid-point values
for one field. Its size ``N`` is determined by the GRIB grid.

Indexing
~~~~~~~~

Index the array with one integer per explicit dimension. The implicit dimension
is typically retrieved as a slice:

.. code-block:: python

   # date index 0 (2020-01-01), time index 2 (1200)
   field = data[0, 2, :]   # returns a 1-D NumPy array of N float32 values

   # date index 1 (2020-01-02), time index 0 (0000)
   field = data[1, 0, :]

**No data is fetched from FDB until you index.** Building the store is cheap —
it validates your MARS request and pre-fetches layout metadata but does not
retrieve field values.

Missing Data and Fill Values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some GRIB fields use a **bitmap** to mark individual grid points as missing
(for example, a sea-surface temperature field where land points have no value).
Z3FDB replaces those bitmap-masked points with a configurable fill value before
returning data to you.

By default the fill value is **NaN**, which means missing points are immediately
detectable with :func:`numpy.isnan`:

.. code-block:: python

   import numpy as np

   field = data[0, 2, :]
   missing = np.isnan(field)

To use a different sentinel — for example ``-999.0`` — call
:meth:`~z3fdb.SimpleStoreBuilder.fill_missing_value` on the builder before calling
``build()``:

.. code-block:: python

   builder = SimpleStoreBuilder()
   builder.add_part(...)
   builder.fill_missing_value(-999.0)   # replaces missing points with -999.0
   store = builder.build()

.. warning::

   If the fill value you choose can also appear as a legitimate field value,
   there is no way to distinguish actual data from fill-value positions after
   retrieval. NaN is the default precisely because it cannot occur as a real
   float32 computation result, making it unambiguous. Only change the fill value
   if you are certain it cannot collide with your data.

.. note::

   The fill value is written into the Zarr array metadata as well, so
   Zarr-aware tools and libraries see the same sentinel when they open the store.

Merging Keywords into One Dimension
------------------------------------

A common pattern is to merge ``date`` and ``time`` into a single datetime axis.
Pass both keys to one :class:`~pychunked_data_view.AxisDefinition`:

.. code-block:: python

   builder.add_part(
       {
           "class": "od", 
           ...,
           "date": ["2020-01-01", "2020-01-02"],
           "time": ["0000", "0600", "1200", "1800"], 
           ...,
       },
       [
           AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),  # -> Dim 0, size 8
       ],
       ExtractorType.GRIB,
   )

This produces shape ``(8, N)``. The **rightmost key varies fastest** (C / NumPy
row-major order): ``time`` cycles through all its values before ``date`` advances.

.. code-block:: text

   Index:  0           1           2           3           4           ...
   date:   2020-01-01  2020-01-01  2020-01-01  2020-01-01  2020-01-02  ...
   time:   0000        0600        1200        1800        0000        ...

   index = date_i × num_times + time_j

.. important::

   Keyword order matters.  ``["date", "time"]`` puts ``time`` as the
   fast axis.  ``["time", "date"]`` would put ``date`` as the fast axis.
   Always verify your indexing formula matches the order you declared.

Combining Surface and Pressure-Level Data
------------------------------------------

When your analysis spans both surface and pressure-level parameters you need two
MARS requests (the ``levtype`` keyword changes between them). Z3FDB lets you
merge them into a single array by calling ``add_part`` twice and declaring which
dimension grows across the two ``parts``.

.. code-block:: python

   import zarr
   from z3fdb import SimpleStoreBuilder, AxisDefinition, Chunking, ExtractorType

   builder = SimpleStoreBuilder()

   # Part 1 — surface parameters (levtype=sfc): 2 params
   builder.add_part(
       {
           "class": "ea",
           "domain": "g",
           "expver": "0001",
           "stream": "oper",
           "type": "an",
           "step": 0,
           "date": ["2020-01-01", "2020-01-02"],
           "time": ["0000", "0600", "1200", "1800"],
           "levtype": "sfc",
           "param": [165, 166],               # 2 surface params
       },
       [
           AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),  # Dim 0 — 8 entries
           AxisDefinition(["param"],        Chunking.SINGLE_VALUE),  # Dim 1 — 2 entries
       ],
       ExtractorType.GRIB,
   )

   # Part 2 — pressure-level parameters (levtype=pl): 2 params × 3 levels = 6 entries
   builder.add_part(
       {
           "class": "ea",
           "domain": "g",
           "expver": "0001",
           "stream": "oper",
           "type": "an",
           "step": 0,
           "date": ["2020-01-01", "2020-01-02"],
           "time": ["0000", "0600", "1200", "1800"],
           "levtype": "pl",
           "param": [131, 132],               # 2 pressure-level params
           "levelist": [500, 850, 1000],       # 3 levels
       },
       [
           AxisDefinition(["date", "time"],       Chunking.SINGLE_VALUE),  # Dim 0 — must match Part 1
           AxisDefinition(["param", "levelist"],  Chunking.SINGLE_VALUE),  # Dim 1 — 6 entries
       ],
       ExtractorType.GRIB,
   )

   # Dim 1 (param) grows: Part 1 contributes indices 0–1, Part 2 contributes 2–7
   builder.extend_on_axis(1)

   store = builder.build()
   data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

The resulting array has shape ``(8, 8, N)`` — 8 datetime steps and 8 entries on the
param dimension (2 sfc + 6 pl).

.. code-block:: python

   # Surface param 165, date=2020-01-01, time=1200 → dim0=2, dim1=0
   field = data[2, 0, :]

   # Pressure-level param 131 at 850 hPa, same time → dim0=2, dim1=3
   # (pl part starts at offset 2; within pl part: param 131 → 0, level 850 → 1
   #  → flat index 0*3 + 1 = 1, global index = 2 + 1 = 3)
   field = data[2, 3, :]

.. seealso::

   :doc:`dimension_mapping` for a full reference on axis mapping, all chunking
   strategies, fill values, and the complete ``multi-part`` data model.

Common Pitfalls
---------------

**MARS request ends with a comma**
   ``"...,param=167,"`` — the trailing comma causes a parse error.
   Omit the comma on the last key–value pair.

**Multi-valued keyword not covered by any AxisDefinition**
   Every MARS keyword that has more than one value **must** appear in exactly one
   ``AxisDefinition``. Z3FDB raises an error at build time if you omit one.

**Wrong array shape**
   If ``data.shape`` does not match what you expect, check the order of your
   ``AxisDefinition`` list — position in the list is the dimension index.

**Large chunk memory use**
   ``Chunking.WHOLE_AXIS`` on several axes can produce chunks of many gigabytes.
   Start with ``Chunking.SINGLE_VALUE`` on every axis and switch to ``WHOLE_AXIS``
   only when you know you always read that axis in full.
   See :doc:`dimension_mapping` for details.
