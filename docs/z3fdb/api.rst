.. SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
.. SPDX-License-Identifier: Apache-2.0

API
===

.. contents::
   :local:
   :depth: 2

Overview
--------

.. autoapimodule:: z3fdb

Exceptions
----------

z3fdb.Z3fdbError
^^^^^^^^^^^^^^^^

.. autoapiexception:: z3fdb.Z3fdbError

Type aliases
------------

z3fdb.MarsSelection
^^^^^^^^^^^^^^^^^^^

.. autoapidata:: z3fdb.MarsSelection

   A MARS request expressed as a mapping.  Keys are MARS keyword names
   (strings).  Values may be:

   * a single ``str``, ``int``, or ``float`` — e.g. ``"step": 0``
   * a list of ``str``, ``int``, or ``float`` — e.g. ``"param": [165, 166]``
   * a MARS range expression passed as a ``str`` — e.g.
     ``"date": "2020-01-01/to/2020-01-04"``

   Example::

       {
           "class": "ea",
           "domain": "g",
           "expver": "0001",
           "stream": "oper",
           "type": "an",
           "date": "2020-01-01/to/2020-01-04",
           "levtype": "sfc",
           "step": 0,
           "param": [167, 131, 132],
           "time": "0/to/21/by/3",
       }

Classes
-------

z3fdb.SimpleStoreBuilder
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoapiclass:: z3fdb.SimpleStoreBuilder
   :members:

z3fdb.AxisDefinition
^^^^^^^^^^^^^^^^^^^^

See :doc:`dimension_mapping` for how axis definitions map MARS keywords
to Zarr dimensions.

.. autoapiclass:: pychunked_data_view.AxisDefinition
   :members:

Enums
-----

z3fdb.Chunking
^^^^^^^^^^^^^^^^^^^

.. autoapiclass:: pychunked_data_view.Chunking
   :members:

.. py:class:: pychunked_data_view.Chunking.FixedSizeChunk(chunkShape)

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
      AxisDefinition(["date"], Chunking.FixedSizeChunk(chunkShape=3))

   See :ref:`dimension_mapping:Chunking` for a full comparison of
   chunking modes and guidance on when to use each one.

z3fdb.ExtractorType
^^^^^^^^^^^^^^^^^^^

.. autoapiclass:: pychunked_data_view.ExtractorType
   :members:

