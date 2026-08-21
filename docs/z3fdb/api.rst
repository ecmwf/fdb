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

Chunking
--------

z3fdb.Chunking
^^^^^^^^^^^^^^

.. autoapiclass:: pychunked_data_view.Chunking
   :members:

.. py:class:: pychunked_data_view.Chunking.FixedSizeChunk(chunk_shape)

   Specifies a custom chunk size along a single axis. This is a frozen
   dataclass nested inside :class:`~pychunked_data_view.Chunking`.

   .. py:attribute:: chunk_shape
      :type: int

      Number of consecutive axis values grouped into each chunk.
      Must be a positive integer that divides the axis length exactly;
      otherwise :meth:`~pychunked_data_view.ChunkedDataViewBuilder.build`
      raises an exception.

   **Example**

   .. code-block:: python

      # Chunk a 12-date axis into groups of 3 (gives 4 chunks)
      AxisDefinition(["date"], Chunking.FixedSizeChunk(chunk_shape=3))

   See :ref:`dimension_mapping:Chunking` for a full comparison of
   chunking modes and guidance on when to use each one.

Extractors
----------

``ExtractorType`` is a namespace class — not an enum — whose nested classes
carry per-extractor configuration.  Pass an *instance* to
:meth:`~z3fdb.SimpleStoreBuilder.add_part`.

z3fdb.ExtractorType.Grib
^^^^^^^^^^^^^^^^^^^^^^^^

.. py:class:: pychunked_data_view.ExtractorType.Grib(*, fdb_config=None)

   Reads full GRIB fields from FDB and decodes them to ``float32`` via eccodes.
   This is the default extractor for standard GRIB data.

   :param fdb_config: Path to an FDB configuration YAML file.
       ``None`` (default) uses the path passed to :class:`~z3fdb.SimpleStoreBuilder`.
   :type fdb_config: pathlib.Path or None

   **Example**

   .. code-block:: python

      builder.add_part(mars_request, axes, ExtractorType.Grib())

      # With an explicit FDB config
      builder.add_part(mars_request, axes, ExtractorType.Grib(fdb_config=Path("/etc/fdb/config.yaml")))

z3fdb.ExtractorType.GribJump
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. py:class:: pychunked_data_view.ExtractorType.GribJump(*, fdb_config=None, gribjump_config=None, chunking=None)

   Reads grid-point values from FDB using GribJump — a library that jumps
   directly to the values inside the GRIB message without performing a full
   decode.

   :param fdb_config: Path to an FDB configuration YAML file.
       ``None`` (default) uses the path passed to :class:`~z3fdb.SimpleStoreBuilder`.
   :type fdb_config: pathlib.Path or None

   :param gribjump_config: Path to a GribJump configuration YAML file.
       ``None`` (default) reads the ``GRIBJUMP_CONFIG_FILE`` environment variable.
   :type gribjump_config: pathlib.Path or None

   :param chunking: How to sub-divide the implicit (grid-point) dimension into
       Zarr chunks.  ``None`` (default) produces a single chunk covering the
       full field.  Pass :class:`~pychunked_data_view.Chunking.FixedSizeChunk`
       to split the implicit axis into equal-sized pieces.
   :type chunking: Chunking.FixedSizeChunk or None

   **Example**

   .. code-block:: python

      # Full field — avoids eccodes decode
      builder.add_part(mars_request, axes, ExtractorType.GribJump())

      # Split the implicit grid-point axis into chunks of 1312
      builder.add_part(mars_request, axes,
                       ExtractorType.GribJump(chunking=Chunking.FixedSizeChunk(1312)))

   .. seealso:: :doc:`gribjump` for a full guide including when to prefer
      GribJump over the standard GRIB extractor.

