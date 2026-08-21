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

Extractors are held via ``std::unique_ptr<Extractor>``. Each ``ViewPart``
owns its extractor exclusively — there is no sharing between parts.

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

``std::unique_ptr`` makes this ownership explicit: each extractor belongs to
exactly one ``ViewPart``, and is destroyed exactly once when the view is
destroyed.

ExtractorDefinition Factory
----------------------------

Extractors are not constructed directly in ``addPart``. Instead, each part
records an ``ExtractorDefinition`` — a lightweight configuration object that
implements a single factory method:

.. code-block:: cpp

   virtual std::unique_ptr<Extractor> buildExtractor(
       const metkit::mars::MarsRequest& request) const = 0;

``ChunkedDataViewBuilder::build()`` calls ``buildExtractor(request)`` once per
part after the MARS request string has been parsed. This defers FDB and
GribJump initialisation to ``build()`` time, so any configuration errors are
raised there rather than in ``addPart``.

``ChunkedDataViewBuilder`` itself is non-copyable (its copy constructor and
copy-assignment operator are explicitly deleted) because it stores
``std::unique_ptr<ExtractorDefinition>`` objects that cannot be duplicated.

Extractor Interface
--------------------

All extractors implement one method called by the core during a chunk access,
and expose a ``DataLayout`` computed eagerly in the constructor:

``DataLayout layout_``
   Set during construction by issuing a sample FDB retrieve for the part's
   MARS request. Records the field's grid-point count, bytes-per-value, and
   chunk shape so that ``ChunkedDataViewBuilder::build()`` can validate axis
   compatibility before committing to the view.

``extractInto(part, chunkBB, intersectionBB, ptr, len)``
   Called during each chunk access to retrieve the fields matching the part's
   MARS request from FDB, decode them (or partially decode), and write the
   float32 values into the provided buffer at the correct offset.

.. seealso::

   :doc:`chunk_access` for how the extractor's ``extractInto`` method is
   invoked as part of the three-step chunk-access pipeline.

GribExtractor
-------------

``GribExtractor`` (Python: :class:`~pychunked_data_view.ExtractorType.Grib`)
reads GRIB messages from FDB and decodes them to ``float32`` via eccodes. The
entire field is decoded for every chunk access; the implicit grid-point dimension
always covers the full field.

GribJumpExtractor
-----------------

``GribJumpExtractor`` (Python: :class:`~pychunked_data_view.ExtractorType.GribJump`)
uses the GribJump library to read grid-point values without performing a full
GRIB decode.  It is useful when the decode overhead of ``GribExtractor``
would be wasteful.

**Configuration**

``GribJumpExtractor`` requires a running GribJump service.  Its configuration
is read from the ``GRIBJUMP_CONFIG_FILE`` environment variable.  The Python
binding also accepts an explicit ``gribjump_config`` path via
:class:`~pychunked_data_view.ExtractorType.GribJump`; if set, the binding
calls ``setenv("GRIBJUMP_CONFIG_FILE", ...)`` before constructing the
``gribjump::GribJump`` object.

**Layout caching**

``GribJumpExtractor`` resolves the concrete ``gribjump::Range`` on the first
``layout()`` call and caches it.  Subsequent calls to ``extractInto()`` reuse
the cached range, so the service is queried only once per part.

.. seealso::

   :doc:`../gribjump` for a user-facing guide on when and how to use
   GribJump-backed extraction.
