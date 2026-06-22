Dimension Mapping and Data Model
=================================

A MARS request defines which data to retrieve from FDB. Each keyword
with more than one value defines an axis and **must** be mapped to a
Zarr dimension via :class:`~pychunked_data_view.AxisDefinition`.
Keywords with a single value **may** also be mapped — useful when MARS
restricts a keyword to one value but you still want it as an explicit
dimension in the resulting array.

From MARS Keywords to Zarr Dimensions
--------------------------------------

Each :class:`~pychunked_data_view.AxisDefinition` passed to
:meth:`~z3fdb.SimpleStoreBuilder.add_part` becomes **exactly one
dimension** in the resulting Zarr array.

- The position of each :class:`~pychunked_data_view.AxisDefinition` in
  the list determines its dimension index in the array.
- An **implicit final dimension** always contains the grid points
  (decoded field values).


One-to-One Mapping
~~~~~~~~~~~~~~~~~~

In the simplest case, each MARS keyword maps to its own Zarr dimension.

.. code-block:: python

   [
       AxisDefinition(["date"], Chunking.SINGLE_VALUE),   # Dim 0
       AxisDefinition(["time"], Chunking.SINGLE_VALUE),   # Dim 1
       AxisDefinition(["param"], Chunking.SINGLE_VALUE),  # Dim 2
   ]

Given ``date=2020-01-01/to/2020-01-03``, ``time=0/6/12/18``, and
``param=165/166/167``, the resulting array has shape ``(3, 4, 3, N)``
where ``N`` is the number of grid points.

Many-to-One Mapping
~~~~~~~~~~~~~~~~~~~

Multiple MARS keywords can be flattened into a single Zarr dimension.
A common use case is merging ``date`` and ``time`` into a unified
datetime axis.

.. code-block:: python

   [
       AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),  # Dim 0
       AxisDefinition(["param"], Chunking.SINGLE_VALUE),         # Dim 1
   ]

The dimension size equals the **product** of the number of values of
each keyword. With ``date`` having 3 values and ``time`` having 4:

.. code-block:: text

   Dimension size = 3 × 4 = 12

The **rightmost key varies fastest** (row-major order, like C and
NumPy defaults). In ``["date", "time"]``, ``time`` cycles through all
its values before ``date`` advances:

.. code-block:: text

   Index:  0    1    2    3    4    5    6    7    8    9   10   11
   date:   d0   d0   d0   d0   d1   d1   d1   d1   d2   d2   d2   d2
   time:   t0   t1   t2   t3   t0   t1   t2   t3   t0   t1   t2   t3

   index = time + date × num_times

.. important::

   The order of keys matters. With ``["time", "date"]``, ``date``
   becomes the fastest-varying keyword instead of ``time``.


Axis Mapping Visualized
~~~~~~~~~~~~~~~~~~~~~~~

.. mermaid::

   graph LR
       subgraph MARS["MARS Request Keywords"]
           date["date (3 values)"]
           time["time (4 values)"]
           param["param (3 values)"]
           step["step (1 value)"]
       end

       subgraph AD["AxisDefinitions"]
           ad0["AxisDefinition 0<br> keys=['date', 'time']"]
           ad1["AxisDefinition 1<br> keys=['param']"]
           ad2["AxisDefinition 2<br> keys=['step']"]
       end

       subgraph Zarr["Zarr Array Dimensions"]
           dim0["Dim 0: datetime<br>size = 3 x 4 = 12"]
           dim1["Dim 1: param<br>size = 3"]
           dim2["Dim 2: step<br>size = 1"]
           dim3["Dim 3: grid points<br>(implicit)"]
       end

       date --> ad0
       time --> ad0
       param --> ad1
       step --> ad2
       ad0 --> dim0
       ad1 --> dim1
       ad2 --> dim2

Coordinate System
-----------------

A ChunkedDataView exposes an ``(N+1)``-dimensional integer index space:

* **Axes 0 … N−1** — one per :class:`~pychunked_data_view.AxisDefinition`.
* **Axis N** — the implicit trailing dimension holding the decoded
  grid-point float32 values for each field.

The array **shape** is::

    (axis0_size, axis1_size, …, axisN-1_size, num_values)

All axis indices are zero-based. Axis sizes are determined by the total
number of distinct values held across all parts (see
`Combining Multiple MARS Requests`_ below).

Chunking
--------

:class:`~pychunked_data_view.Chunking` determines how many values along
a dimension are grouped into a single Zarr chunk:

.. list-table::
   :header-rows: 1

   * - Chunking mode
     - Behaviour
     - Chunk size along axis
   * - :attr:`~pychunked_data_view.Chunking.SINGLE_VALUE`
     - Each value along the axis is its own chunk
     - 1
   * - :attr:`~pychunked_data_view.Chunking.NONE`
     - The entire axis is stored in a single chunk
     - Full axis length
   * - :class:`~pychunked_data_view.Chunking.IndividualChunk` ``(chunkShape=k)``
     - Groups every ``k`` consecutive values along the axis into one chunk.
       ``k`` must divide the axis length evenly.
     - ``k`` (user-specified)

For example, with ``date`` having 4 values and ``param`` having 3
values:

.. code-block:: python

   [
       AxisDefinition(["date"], Chunking.NONE),                         # chunk size = 4
       AxisDefinition(["param"], Chunking.SINGLE_VALUE),                # chunk size = 1
   ]
   # Array shape:  (4, 3, N)
   # Chunk shape:  (4, 1, N)

   [
       AxisDefinition(["date"], Chunking.IndividualChunk(chunkShape=2)),  # chunk size = 2
       AxisDefinition(["param"], Chunking.SINGLE_VALUE),                  # chunk size = 1
   ]
   # Array shape:  (4, 3, N)
   # Chunk shape:  (2, 1, N)   ← two dates per chunk, four chunks total

:class:`~pychunked_data_view.Chunking.IndividualChunk` is useful when
neither extreme fits — for instance, when you want to batch a temporal
axis into multi-day windows for efficient I/O while still keeping chunks
small enough to fit in memory.

A chunk is addressed by a *chunk-index tuple* ``(c0, c1, …, cN-1)`` —
one integer per MARS axis. The implicit values dimension is never
chunked. Each chunk index ``ci`` maps to an axis range:

.. list-table::
   :header-rows: 1

   * - Chunking
     - Axis range covered by chunk index ``ci``
   * - ``SINGLE_VALUE``
     - ``[ci, ci]`` — exactly one slot
   * - ``NONE``
     - ``[0, size_i − 1]`` — the full axis (``ci`` is always 0)
   * - ``IndividualChunk(chunkShape=k)``
     - ``[ci × k, (ci + 1) × k − 1]`` — a window of ``k`` consecutive values

The chunk's **bounding box** is the Cartesian product of these per-axis
ranges. Its flat memory footprint is::

    chunkSize0 × chunkSize1 × … × chunkSizeN-1 × num_values  float32 values

Memory Considerations
---------------------

Each chunk access loads the **entire chunk** into memory. With
:attr:`~pychunked_data_view.Chunking.SINGLE_VALUE` each chunk contains
one set of grid-point values, keeping memory usage small.
With :attr:`~pychunked_data_view.Chunking.NONE` the chunk spans the
full axis, and when multiple axes use ``NONE`` the chunk sizes compound.

For example, consider a grid with 1 million points (``N = 1_000_000``)
and three axes all set to ``NONE``:

.. code-block:: python

   [
       AxisDefinition(["date"], Chunking.NONE),   # 30 values
       AxisDefinition(["time"], Chunking.NONE),   # 4 values
       AxisDefinition(["param"], Chunking.NONE),   # 10 values
   ]
   # Chunk shape: (30, 4, 10, 1_000_000)
   # Chunk size:  30 × 4 × 10 × 1_000_000 × 4 bytes = ~4.5 GB

Accessing **any** element in this array loads the single 4.5 GB chunk.
Switching to :attr:`~pychunked_data_view.Chunking.SINGLE_VALUE` on all
three axes reduces each chunk to a single field
(``1 × 1 × 1 × 1_000_000 × 4 bytes ≈ 4 MB``).

.. warning::

   Using :attr:`~pychunked_data_view.Chunking.NONE` on multiple axes
   can cause unexpectedly large memory allocations. Start with
   :attr:`~pychunked_data_view.Chunking.SINGLE_VALUE` on all axes and
   only switch individual axes to ``NONE`` when you know you always
   consume them in full.

.. rubric:: Choosing a chunking strategy

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Mode
     - When to use
   * - ``SINGLE_VALUE``
     - Default choice. Minimises memory per access; ideal when you read
       individual time steps or parameters one at a time.
   * - ``NONE``
     - Use when you always read the full axis in one go and want to
       reduce the number of FDB round-trips (e.g. a small ``step`` axis
       you always load entirely).
   * - ``IndividualChunk(chunkShape=k)``
     - Use when you need a middle ground — for example, batching a
       365-day date axis into weekly (``k=7``) or monthly (``k=30``)
       windows. ``k`` must divide the axis length exactly.

Fill Value
----------

When a chunk is accessed and some of its fields are absent from FDB,
the missing slots are filled with a sentinel value. The default is
positive infinity (``float('inf')``). You can override this via
:meth:`~pychunked_data_view.ChunkedDataViewBuilder.fill_value`:

.. code-block:: python

   builder = SimpleStoreBuilder()
   builder.add_part(...)
   builder.fill_value(float("nan"))   # use NaN instead of inf
   store = builder.build()

The effective fill value is also available on the resulting view:

.. code-block:: python

   store = builder.build()
   print(store.fillValue())  # float('inf') unless overridden

Combining Multiple MARS Requests
---------------------------------

Call :meth:`~z3fdb.SimpleStoreBuilder.add_part` multiple times to
combine data from different MARS requests into a single Zarr array.
Use :meth:`~z3fdb.SimpleStoreBuilder.extendOnAxis` to specify which
dimension grows when parts are joined. All other dimensions must have
the same number of values across parts.

.. code-block:: python

   builder = SimpleStoreBuilder()

   # Part 1: surface parameters
   # Dimension D is count date x time
   # Dimension P1 is count param
   # Dimension N is the number of values in the grid
   # Resulting shape of this part is [D, P1, N]
   builder.add_part(
       "levtype=sfc,param=165/166,...",
       [
           AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
           AxisDefinition(["param"], Chunking.SINGLE_VALUE),
       ],
       ExtractorType.GRIB,
   )

   # Part 2: pressure level parameters
   # Dimension D is count date x time
   # Dimension P2 is count param x levelist
   # Dimension N is the number of values in the grid
   # Resulting shape of this part is [D, P2, N]
   builder.add_part(
       "levtype=pl,param=131/132,levelist=50/100,...",
       [
           AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
           AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
       ],
       ExtractorType.GRIB,
   )

   # Extend on the param dimension (index 1)
   # Final shape will be [D, P1 + P2, N]
   builder.extendOnAxis(1)
   store = builder.build()

The datetime dimension (index 0) must have the same values in both
parts. The param dimension (index 1) grows: 2 surface parameters +
4 pressure-level combinations (2 params × 2 levels) = 6 entries total.

Each Part occupies a rectangular sub-region of the global index space
described by a closed bounding box — one ``[lower_i, upper_i]`` interval
per axis (both bounds **inclusive**). Parts tile the extension axis
without overlap; their bounding boxes are identical on every other axis.

.. code-block:: text

    Global index space after extend_on_axis(1):

    Axis 0 (date×time) varies along rows (0–3).
    Axis 1 (param) varies along columns (0–5).

               0   1   2   3   4   5
             ┌───┬───┬───┬───┬───┬───┐
          0  │ A │ A │ B │ B │ B │ B │
             ├───┼───┼───┼───┼───┼───┤
          1  │ A │ A │ B │ B │ B │ B │
             ├───┼───┼───┼───┼───┼───┤
          2  │ A │ A │ B │ B │ B │ B │
             ├───┼───┼───┼───┼───┼───┤
          3  │ A │ A │ B │ B │ B │ B │
             └───┴───┴───┴───┴───┴───┘
             └─ A ──┘└───── B ──────┘

    Part A (sfc, 2 params)  bounding box: axis0=[0,3], axis1=[0,1]
    Part B (pl,  4 params)  bounding box: axis0=[0,3], axis1=[2,5]

.. seealso::

   :doc:`technical_insights` — how the library resolves a chunk access
   into FDB sub-requests and writes each field into the correct buffer slot.

