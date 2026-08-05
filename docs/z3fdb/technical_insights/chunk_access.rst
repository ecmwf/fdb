.. _z3fdb_chunk_access:

Chunk Access Pipeline
=====================

When a Zarr chunk is read from a Z3FDB store, the library executes three
steps for every ``Part`` (one per :meth:`~z3fdb.SimpleStoreBuilder.add_part`
call):

1. **Intersection** — compute the overlap between the requested chunk's
   bounding box and the ``Part``'s bounding box. Parts with no overlap are
   skipped immediately.
2. **FDB retrieve** — issue a sub-request to FDB for exactly the fields
   inside the intersection. Fields outside it are never fetched.
3. **Buffer fill** — decode each returned GRIB field to ``float32`` and write
   it into the correct position in the flat chunk buffer (row-major / C order).

The examples below use the two-part view from :doc:`/z3fdb/dimension_mapping`:
**Part A** covers surface parameters (``sfc``, 2 params, axis 1 = [0, 1]) and
**Part B** covers pressure-level parameters (``pl``, 4 params, axis 1 = [2, 5]).
Both parts share 4 date × time values on axis 0.


``WHOLE_AXIS`` chunking
-----------------------

With ``WHOLE_AXIS`` on all axes there is exactly one chunk covering the entire
array. Both parts contribute to it, each populating a disjoint rectangular
region of the buffer.

.. code-block:: text

    Chunk (0, 0) bounding box: axis0 = [0, 3], axis1 = [0, 5]

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

    Intersection A:  axis0 = [0, 3], axis1 = [0, 1]  (left two columns)
    Intersection B:  axis0 = [0, 3], axis1 = [2, 5]  (right four columns)

    Buffer extent: [4, 6]

    Part A — partAxisOffset = [0, 0], bufferOffset = [0, 0]
              The intersection starts at A's local origin and at the buffer corner.

    Part B — partAxisOffset = [0, 0], bufferOffset = [0, 2]
              The intersection starts at B's local origin but at buffer column 2,
              because B begins at global index 2 on axis 1.


``SINGLE_VALUE`` chunking
--------------------------

With ``SINGLE_VALUE`` every chunk holds exactly one field. Accessing chunk
``(1, 2)`` targets one cell that falls entirely inside Part B.

.. code-block:: text

    Chunk (1, 2) bounding box: axis0 = [1, 1], axis1 = [2, 2]

               0   1   2   3   4   5
             ┌───┬───┬───┬───┬───┬───┐
          0  │   │   │   │   │   │   │
             ├───┼───┼───┼───┼───┼───┤
          1  │   │   │ ■ │   │   │   │   ← chunk (1, 2)
             ├───┼───┼───┼───┼───┼───┤
          2  │   │   │   │   │   │   │
             ├───┼───┼───┼───┼───┼───┤
          3  │   │   │   │   │   │   │
             └───┴───┴───┴───┴───┴───┘

    Intersection with Part A: empty — skipped.
    Intersection with Part B: axis0 = [1, 1], axis1 = [2, 2]

    Part B — partAxisOffset = [1, 0], bufferOffset = [0, 0], bufferExtent = [1, 1]

        axis 0: partAxisOffset = 1 because the intersection starts at
                date×time index 1 within Part B's local axis.
        axis 1: partAxisOffset = 0 because global param index 2 is the
                first value in Part B's local param axis (B starts at
                global index 2).
        bufferOffset = [0, 0] because the intersection coincides with
                the chunk's own lower-left corner.

    FDB returns one field. Within Part B, axis.index(key) = [1, 0]:

        axis 0: local = 1 − 1 = 0,   bufPos = 0 + 0 = 0
        axis 1: local = 0 − 0 = 0,   bufPos = 0 + 0 = 0
        → written to buffer slot (0, 0)

``FixedSizeChunking`` — cross-part chunk example
-------------------------------------------------

With ``FixedSizeChunk(2)`` on axis 0 and ``FixedSizeChunk(3)`` on axis 1, the
chunk grid is 2 × 2. Chunk ``(0, 0)`` covers two date×time steps and the first
three param slots, which straddles the boundary between Part A and Part B.

.. code-block:: text

    Chunking:  axis0 = FixedSizeChunk(2),  axis1 = FixedSizeChunk(3)

    Chunk (0, 0) bounding box: axis0 = [0, 1], axis1 = [0, 2]

               0   1   2   3   4   5
             ┌───┬───┬───┬───┬───┬───┐
          0  │ A │ A │ B │   │   │   │  ← rows covered by chunk (0, 0)
             ├───┼───┼───┼───┼───┼───┤
          1  │ A │ A │ B │   │   │   │
             ├───┼───┼───┼───┼───┼───┤
          2  │   │   │   │   │   │   │
             ├───┼───┼───┼───┼───┼───┤
          3  │   │   │   │   │   │   │
             └───┴───┴───┴───┴───┴───┘
             └─  (0,0)  ─┘

    Intersection A:  axis0 = [0, 1], axis1 = [0, 1]   (left 2 columns)
    Intersection B:  axis0 = [0, 1], axis1 = [2, 2]   (third column only)

    Buffer extent: [2, 3]

    Part A — partAxisOffset = [0, 0], bufferOffset = [0, 0]
              Intersection starts at A's local origin and at the buffer corner.

    Part B — partAxisOffset = [0, 0], bufferOffset = [0, 2]
              B's local axis1 starts at global index 2, so global [2, 2]
              maps to local [0, 0]. The intersection lands at buffer column 2
              because 2 − 0 (chunk lower bound) = 2.

    Buffer layout (2 rows × 3 columns):

               0   1   2
             ┌───┬───┬───┐
          0  │ A │ A │ B │
             ├───┼───┼───┤
          1  │ A │ A │ B │
             └───┴───┴───┘

FDB issues two sub-requests — one for Part A, one for Part B. Each field is
placed using the buffer-position formula. For a field returned by Part A with
``axis.index(key) = [1, 1]`` (second date×time, second sfc param):

.. code-block:: text

        axis 0: local = 1 − 0 = 1,   bufPos = 1 + 0 = 1
        axis 1: local = 1 − 0 = 1,   bufPos = 1 + 0 = 1
        → written to buffer slot (1, 1)

For a field returned by Part B with ``axis.index(key) = [0, 0]`` (first
date×time, first pl param — which is global param index 2):

.. code-block:: text

        axis 0: local = 0 − 0 = 0,   bufPos = 0 + 0 = 0
        axis 1: local = 0 − 0 = 0,   bufPos = 0 + 2 = 2
        → written to buffer slot (0, 2)

.. seealso::

   :doc:`buffer_layout` for the general buffer-position formula and how
   the flat buffer index is computed from the per-axis positions.
