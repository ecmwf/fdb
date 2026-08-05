.. _z3fdb_buffer_layout:

Buffer Layout and Position Formula
====================================

Every chunk is backed by a flat ``float32`` array in memory. When fields are
returned by FDB they are decoded and written into this array one at a time.
This page explains how the correct write position is calculated.

Chunk Buffer Shape
------------------

The chunk buffer is a C-order (row-major) multi-dimensional array with one
entry per combination of axis positions inside the chunk, times the number of
grid-point values ``num_values``:

.. code-block:: text

    total floats = chunkSize_0 × chunkSize_1 × … × chunkSize_{N-1} × num_values

``chunkSize_i`` depends on the chunking mode chosen for axis ``i``:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Chunking mode
     - ``chunkSize_i``
   * - ``SINGLE_VALUE``
     - 1
   * - ``WHOLE_AXIS``
     - Full axis length
   * - ``FixedSizeChunk(k)``
     - ``k``

The grid-point dimension (``num_values``) is always the trailing dimension and
is never chunked.

Buffer Position Formula
-----------------------

For every GRIB field returned by FDB during a chunk access, the buffer write
position along each axis ``i`` is:

.. code-block:: text

    local_i   = axis.index(key_i) − partAxisOffset[i]
    bufPos_i  = local_i + bufferOffset[i]

Where:

``axis.index(key_i)``
   Zero-based position of the MARS key value returned by FDB within the
   ``Part``'s local axis (range: ``0`` to ``axisSize_i − 1``).

``partAxisOffset[i]``
   Start of the intersection within the ``Part``'s own local axis::

       partAxisOffset[i] = intersection.lower[i] − partBoundingBox.lower[i]

``bufferOffset[i]``
   Start of the intersection within the chunk buffer::

       bufferOffset[i] = intersection.lower[i] − chunkBoundingBox.lower[i]

``local_i`` is the zero-based position of the field *within the intersection*
along axis ``i``. Adding ``bufferOffset[i]`` shifts it to the correct slot
in the chunk buffer.

Flat Buffer Index
-----------------

The per-axis positions are combined into a single flat index using C-order
(row-major) arithmetic — the rightmost axis varies fastest:

.. code-block:: text

    stride_{N-1}  = num_values
    stride_{N-2}  = chunkSize_{N-1} × num_values
    stride_{N-3}  = chunkSize_{N-2} × stride_{N-2}
    …

    flatIndex = sum(bufPos_i × stride_i  for i in 0..N-1)

Each position in the flat index corresponds to ``num_values`` consecutive
``float32`` values — the decoded grid-point values for that field.

.. note::

   The formula applies independently to every field FDB returns. Fields are
   written into the buffer in whatever order FDB returns them; the position
   formula makes the result independent of retrieval order.

.. seealso::

   :doc:`chunk_access` for the intersection and FDB-retrieve steps that
   precede the buffer fill.
