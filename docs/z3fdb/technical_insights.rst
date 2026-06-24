Technical Insights
==================

.. note::

   This page describes Z3FDB's internal mechanics. It is aimed at contributors
   and anyone debugging unexpected performance behaviour. If you are using
   Z3FDB for the first time, start with :doc:`getting_started` instead.

This page covers internal mechanics that are useful when reasoning about
performance, debugging unexpected results, or contributing to the library.

Part, Intersection, and Buffer Layout
--------------------------------------

When a chunk is accessed the library executes these steps for every Part:

1. **Intersection** — compute the overlap of the chunk bounding box and
   the Part bounding box. Parts with no overlap are skipped.
2. **FDB retrieve** — send a sub-request to FDB for exactly the fields
   inside the intersection.
3. **Buffer fill** — decode each returned field (GRIB → float32) and
   write it at its correct position in the flat chunk buffer
   (row-major / C order).

The examples below use the two-part view introduced in
:doc:`dimension_mapping`: Part A (sfc, 2 params, axis1=[0,1]) and Part B
(pl, 4 params, axis1=[2,5]) with 4 date×time values on axis 0.

``NONE`` chunking — whole-view example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With ``NONE`` on all axes there is one chunk covering the entire array.
Both parts are queried and each populates a disjoint region of the buffer.

.. code-block:: text

    Chunk (0,0) bounding box: axis0=[0,3], axis1=[0,5]

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

    Intersection A = axis0=[0,3], axis1=[0,1]   (left two columns)
    Intersection B = axis0=[0,3], axis1=[2,5]   (right four columns)

    The chunk buffer mirrors the index space exactly:

               0   1   2   3   4   5
             ┌───┬───┬───┬───┬───┬───┐
          0  │ A │ A │ B │ B │ B │ B │
             ├───┼───┼───┼───┼───┼───┤
          1  │ A │ A │ B │ B │ B │ B │   bufferExtent = [4, 6]
             ├───┼───┼───┼───┼───┼───┤
          2  │ A │ A │ B │ B │ B │ B │
             ├───┼───┼───┼───┼───┼───┤
          3  │ A │ A │ B │ B │ B │ B │
             └───┴───┴───┴───┴───┴───┘

    Part A — partAxisOffset=[0,0], bufferOffset=[0,0]
              (intersection starts at A's local origin and at buffer corner)

    Part B — partAxisOffset=[0,0], bufferOffset=[0,2]
              (intersection starts at B's local origin but at buffer column 2)

``SINGLE_VALUE`` chunking — single-field example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With ``SINGLE_VALUE`` every chunk holds exactly one field. Chunk ``(1,2)``
covers one cell that falls inside Part B.

.. code-block:: text

    Chunk (1,2) bounding box: axis0=[1,1], axis1=[2,2]

               0   1   2   3   4   5
             ┌───┬───┬───┬───┬───┬───┐
          0  │   │   │   │   │   │   │
             ├───┼───┼───┼───┼───┼───┤
          1  │   │   │ ■ │   │   │   │   ← chunk (1,2)
             ├───┼───┼───┼───┼───┼───┤
          2  │   │   │   │   │   │   │
             ├───┼───┼───┼───┼───┼───┤
          3  │   │   │   │   │   │   │
             └───┴───┴───┴───┴───┴───┘

    Intersection with Part B = axis0=[1,1], axis1=[2,2]

    Part B — partAxisOffset=[1,0], bufferOffset=[0,0], bufferExtent=[1,1]

       axis 0:  partAxisOffset=1 because the intersection starts at
                date×time index 1 within Part B's local axis.
       axis 1:  partAxisOffset=0 because global param index 2 is the first
                value in Part B's local param axis (B starts at global 2).
       bufferOffset=[0,0] because the intersection coincides with the
                chunk's own lower corner.

    FDB returns one field; axis.index(key) within Part B = [1, 0]:
       axis 0:  local = 1 − 1 = 0,   bufPos = 0 + 0 = 0
       axis 1:  local = 0 − 0 = 0,   bufPos = 0 + 0 = 0
       -> written to buffer slot (0, 0)

Buffer Position Formula
~~~~~~~~~~~~~~~~~~~~~~~

For every field returned by FDB, the buffer slot along each axis ``i`` is:

.. code-block:: text

    local_i  = axis.index(key) − partAxisOffset[i]
    bufPos_i = local_i + bufferOffset[i]

Where:

* **axis.index(key)** — zero-based position of the key within the Part's
  local axis (0 to ``axisSize − 1``).
* **partAxisOffset[i]** — start of the intersection within the Part's local
  axis: ``intersection.lower[i] − partBoundingBox.lower[i]``.
* **bufferOffset[i]** — start of the intersection within the chunk buffer:
  ``intersection.lower[i] − chunkBoundingBox.lower[i]``.
* **bufferExtent[i]** — total size of the chunk buffer along axis ``i``
  (= chunk size on that axis).

``local_i`` is the zero-based position *within the intersection*; adding
``bufferOffset[i]`` shifts it to the correct slot in the chunk buffer. The
flat buffer index is then the C-order (row-major) combination of all
``bufPos_i`` values, with each slot holding ``num_values`` consecutive
floats.
