.. SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
.. SPDX-License-Identifier: Apache-2.0

.. _z3fdb_extractor:

Extractor Ownership and Lifetime
=================================

Each ``ViewPart`` (one per :meth:`~z3fdb.SimpleStoreBuilder.add_part` call)
holds a reference to an ``Extractor`` object. The ``Extractor`` is responsible
for issuing the FDB sub-request and decoding the returned data into the chunk
buffer.

Ownership Model
---------------

Extractors are held via ``std::shared_ptr<Extractor>``. This enables two
important properties:

**Sharing across parts**
   Multiple ``ViewPart`` objects can share a single ``Extractor`` instance —
   for example, two parts reading from the same FDB store can reuse the same
   open handle without duplicating it.

**Tied lifetime**
   Each extractor's lifetime is bound to the ``ChunkedDataView`` that owns it.
   As long as the view is alive, every extractor it references remains alive.
   Destroying the view closes all extractor handles cleanly.

Why Extractors Are Non-Copyable
--------------------------------

Concrete extractor implementations are **stateful and non-copyable**. An
FDB-backed extractor owns an open FDB connection handle. Copying such a handle
would duplicate a live network or file-system connection, which is unsafe —
both copies would race on the same underlying state.

``std::shared_ptr`` provides shared ownership without copying: all parts that
reference the same extractor share one instance, and the instance is destroyed
exactly once when the last shared reference is dropped.

Extractor Interface
--------------------

All extractors implement two methods called by the core during a chunk access:

``layout(request)``
   Called once per part during :meth:`~z3fdb.SimpleStoreBuilder.build` to
   probe the field layout — grid size and axis ordering — without reading
   actual data values.

``extract(request, buffer, offset)``
   Called during each chunk access to retrieve the fields matching *request*
   from FDB, decode them, and write them into *buffer* at *offset*.

The only extractor currently shipped is ``GribExtractor``, which reads GRIB
messages from FDB and decodes them to ``float32`` via eccodes.

.. seealso::

   :doc:`chunk_access` for how the extractor's ``extract`` method is invoked
   as part of the three-step chunk-access pipeline.
