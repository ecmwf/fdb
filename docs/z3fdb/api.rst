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

Extractor errors
^^^^^^^^^^^^^^^^

Raised by the extractor backends and re-exported from
:mod:`pychunked_data_view`, so they can be caught by type:

``GribExtractorError``
   A GRIB field could not be retrieved or decoded. For example, FDB returned no field
   for a sub-request, or a field's size does not match the rest of the view.

``GribJumpExtractorError``
   GribJump extraction failed. For example, a field location carried no usable file
   offset, or the request matched nothing.

``MarsRequestFormattingError``
   A malformed MARS request string: a trailing comma, a missing comma between keys, or a
   misspelled key. Raised from ``build()``; a subclass of ``RuntimeError``.

``InternalError``
   Something inside ``pychunked_data_view`` is inconsistent. You should not see this.

Note that other misconfiguration detected by the builder (unmapped axes, incompatible parts, an
invalid chunk size, parts disagreeing about the grid) surfaces as a plain ``RuntimeError``,
since it originates as an ``eckit::UserError``.

Build capability
^^^^^^^^^^^^^^^^

.. py:data:: pychunked_data_view.has_gribjump_extractor
   :type: bool

   Whether this build compiled the GribJump extractor.
   :class:`~pychunked_data_view.ExtractorType.GribJump` can always be constructed, so this is
   the way to find out whether it can actually be used. See
   :ref:`z3fdb_gribjump_availability`.

Type aliases
------------

z3fdb.MarsSelection
^^^^^^^^^^^^^^^^^^^

.. autoapidata:: z3fdb.MarsSelection

   A MARS request expressed as a mapping.  Keys are MARS keyword names
   (strings).  Values may be:

   * a single ``str``, ``int``, or ``float``, e.g. ``"step": 0``
   * a list of ``str``, ``int``, or ``float``, e.g. ``"param": [165, 166]``
   * a MARS range expression passed as a ``str``, e.g.
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

Creates a store whose root *is* the array. Equivalent to
:class:`~z3fdb.CustomStoreBuilder` restricted to ``path=None``, which is what it
delegates to.

.. autoapiclass:: z3fdb.SimpleStoreBuilder
   :members:

z3fdb.ChunkedDataView
^^^^^^^^^^^^^^^^^^^^^

The read-only array returned by ``build()`` on the lower-level
:class:`~pychunked_data_view.ChunkedDataViewBuilder`. Zarr normally drives it for you.

.. note::

   ``chunkShape()`` is deprecated in favour of ``chunk_shape()``; it still works but emits a
   ``DeprecationWarning``. Every other accessor on the class is already snake_case.

.. autoapiclass:: pychunked_data_view.ChunkedDataView
   :members:

z3fdb.CustomStoreBuilder
^^^^^^^^^^^^^^^^^^^^^^^^

Creates a store with an arbitrary group/array hierarchy: every method takes a
zarr-style *path* naming the array it applies to, and ``path=None`` addresses a
root array (mutually exclusive with any named path).

.. seealso:: :ref:`tutorial_custom_store_mixed_extractors` for a worked example
   building several arrays with different extractors in one store.

.. autoapiclass:: z3fdb.CustomStoreBuilder
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

   See :ref:`z3fdb_chunking` for a full comparison of
   chunking modes and guidance on when to use each one.

Extractors
----------

``ExtractorType`` is a namespace class, not an enum. Its nested classes
carry per-extractor configuration.  Pass an *instance* to
:meth:`~z3fdb.SimpleStoreBuilder.add_part`.

``add_part`` stores a *copy* of the configuration, so one instance can be reused across as many
parts and builders as you like, and the ``fdb_config`` a builder fills in for you is never
written back into your object.

.. seealso:: :ref:`z3fdb_extractor_backends` for what the two backends do, their constraints,
   and which builds provide GribJump.

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

.. py:class:: pychunked_data_view.ExtractorType.GribJump(*, fdb_config=None, gribjump_config=None, field_chunking=None)

   Reads grid-point values from FDB using GribJump, a library that jumps
   directly to the values inside the GRIB message without performing a full
   decode.

   :param fdb_config: Path to an FDB configuration YAML file.
       ``None`` (default) uses the path passed to :class:`~z3fdb.SimpleStoreBuilder`.
   :type fdb_config: pathlib.Path or None

   :param gribjump_config: Path to a GribJump configuration YAML file.
       ``None`` (default) reads the ``GRIBJUMP_CONFIG_FILE`` environment variable.
   :type gribjump_config: pathlib.Path or None

   :param field_chunking: How to sub-divide the implicit (grid-point) dimension into
       Zarr chunks.  ``None`` (default) produces a single chunk covering the
       full field.  Pass :class:`pychunked_data_view.Chunking.FixedSizeChunk`
       to split the implicit axis into equal-sized pieces; the size must divide
       the grid exactly, as that dimension cannot be left ragged.
   :type field_chunking: pychunked_data_view.Chunking.FixedSizeChunk or None

   **Example**

   .. code-block:: python

      # Full field: avoids eccodes decode
      builder.add_part(mars_request, axes, ExtractorType.GribJump())

      # Split the implicit grid-point axis into chunks of 1312
      builder.add_part(mars_request, axes,
                       ExtractorType.GribJump(field_chunking=Chunking.FixedSizeChunk(1312)))

   .. seealso:: :ref:`z3fdb_extractor_backends` for how the two backends differ, their
      constraints, and which builds provide GribJump; and
      :ref:`tutorial_custom_store_mixed_extractors` for a worked example using both.

