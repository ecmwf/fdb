Dimension Mapping
=================

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

For example, with ``date`` having 3 values and ``param`` having 3
values:

.. code-block:: python

   [
       AxisDefinition(["date"], Chunking.NONE),          # chunk size = 3
       AxisDefinition(["param"], Chunking.SINGLE_VALUE), # chunk size = 1
   ]
   # Array shape:  (3, 3, N)
   # Chunk shape:  (3, 1, N)

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
