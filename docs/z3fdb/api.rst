z3FDB - API
===========

.. contents::
   :local:
   :depth: 2

Overview
--------

.. autoapimodule:: z3fdb

Exceptions
----------

z3fdb.Z3fdbError
................

.. autoapiexception:: z3fdb.Z3fdbError

Classes
-------

z3fdb.SimpleStoreBuilder
........................

.. autoapiclass:: z3fdb.SimpleStoreBuilder
   :members:

z3fdb.AxisDefinition
....................

See :doc:`dimension_mapping` for how axis definitions map MARS keywords
to Zarr dimensions.

.. autoapiclass:: pychunked_data_view.AxisDefinition
   :members:

Enums
-----

z3fdb.Chunking
...................

.. autoapiclass:: pychunked_data_view.Chunking
   :members:

.. py:class:: pychunked_data_view.Chunking.IndividualChunk(chunkShape)

   Specifies a custom chunk size along a single axis. This is a frozen
   dataclass nested inside :class:`~pychunked_data_view.Chunking`.

   .. py:attribute:: chunkShape
      :type: int

      Number of consecutive axis values grouped into each chunk.
      Must be a positive integer that divides the axis length exactly;
      otherwise :meth:`~pychunked_data_view.ChunkedDataViewBuilder.build`
      raises an exception.

   **Example**

   .. code-block:: python

      # Chunk a 12-date axis into groups of 3 (gives 4 chunks)
      AxisDefinition(["date"], Chunking.IndividualChunk(chunkShape=3))

   See :ref:`dimension_mapping:Chunking` for a full comparison of
   chunking modes and guidance on when to use each one.

z3fdb.ExtractorType
...................

.. autoapiclass:: pychunked_data_view.ExtractorType
   :members:

Fill Value
----------

.. py:method:: pychunked_data_view.ChunkedDataViewBuilder.fill_value(value: float)

   Sets the fill value written into buffer slots for which no data
   exists in FDB. Must be called before
   :meth:`~pychunked_data_view.ChunkedDataViewBuilder.build`.

   :param value: Sentinel value for missing fields.
                 Defaults to ``float('inf')`` when not called.
   :type value: float

   .. code-block:: python

      builder = SimpleStoreBuilder()
      builder.add_part(...)
      builder.fill_value(float("nan"))
      store = builder.build()

.. py:method:: pychunked_data_view.ChunkedDataView.fillValue() -> float

   Returns the fill value configured for this view.

   :returns: The sentinel value used for missing fields
             (``float('inf')`` unless overridden via
             :meth:`~pychunked_data_view.ChunkedDataViewBuilder.fill_value`).
   :rtype: float
