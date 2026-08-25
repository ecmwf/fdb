===================
FDB RADOS backend
===================

Overview
========

The RADOS backend stores an FDB database in a Ceph object store through
eckit's RADOS API. It separates the database into two kinds of persistent
state:

* **Catalogue metadata**: database identity, schema, index references, index
  entries, and axis values.
* **Field data**: the encoded field payloads archived by FDB.

The backend is selected with the ``rados`` store and catalogue type. A
database key is mapped to one Ceph pool and one RADOS namespace. The namespace
contains both the catalogue metadata and the field objects for that database.

.. mermaid::

   flowchart TD
       FDB[FDB API] --> C[Catalogue writer/reader]
       FDB --> S[Store writer/reader]
       C --> KV[Catalogue RADOS KV]
       C --> IKV[Index and axis RADOS KVs]
       S --> OBJ[Field RADOS objects]
       KV --> NS[Ceph pool and database namespace]
       IKV --> NS
       OBJ --> NS

Build-time enablement
=====================

RADOS support is optional. FDB enables it when both eckit RADOS support and
the Ceph RADOS development library are available:

.. code-block:: console

   cmake <bundle-source> \
     -DENABLE_RADOS=ON \
     -DENABLE_RADOSFDB=ON

``HAVE_RADOSFDB`` controls whether the RADOS source files and tests are added
to the build. The generated ``fdb5_config.h`` exposes the corresponding
``fdb5_HAVE_RADOSFDB`` feature macro. RADOS tests are compiled only when the
backend is enabled.

The separate ``RADOS_TESTS_MANAGE_POOLS`` option affects test setup only. It
allows tests to create and destroy their own pool; it does not change the
production backend.

Placement and configuration
===========================

RADOS placement is configured through ``spaces[].roots[]``. The first
``spaces`` entry whose ``regex`` matches the database key is selected. The
current implementation requires exactly one root in the matching space.

.. code-block:: yaml

   spaces:
   - regex: ".*"
     roots:
     - pool: fdb-rados
       root_namespace: fdb-root
       namespace_prefix: fdb

The root attributes have these meanings:

* ``pool``: Ceph pool used for the database.
* ``root_namespace``: the **registry namespace**, a shared RADOS namespace
  containing the ``main_kv`` registry for the space.
* ``namespace_prefix``: prefix used to derive the **database namespace**, the
  RADOS namespace containing one database's catalogue and field objects.

Unlike filesystem-backed FDB engines, the RADOS backend does not use a root
filesystem path. The generic FDB configuration model permits ``path`` in
``spaces[].roots[]``, but it is not needed for RADOS placement and is not read
by the RADOS engine.

For a database key whose values serialize as ``11:22``, the database namespace
is ``fdb_11:22``. The namespace prefix must not contain ``_``, because the
underscore separates the prefix from the serialized database key. The
``root_namespace`` and the derived database namespace are different: the
registry namespace contains ``main_kv``, while the database namespace contains
``catalogue_kv``, index KVs, and field objects.

The optional ``rados`` block currently provides the maximum multipart object part size:

.. code-block:: yaml

   rados:
    maxPartSize: 67108864

The value is expressed in bytes. A value of zero uses eckit's default behavior
for the multipart write handle.

RADOS layout
============

For the example above, the catalogue is represented by::

   rados:fdb-rados/fdb_11:22/catalogue_kv

The ``main_kv`` object in the registry namespace ``fdb-root`` maps the database
namespace to this catalogue URI. This registry allows a database to be found
again when a catalogue is opened by its FDB key.

The ``catalogue_kv`` object contains:

* ``key``: serialized FDB database key.
* ``schema``: serialized schema used by the database.
* One entry per index key, whose value is the URI of the index RADOS KV.
* ``control.*`` entries for persisted control state, such as list and
  retrieve visibility.

Each index is a RADOS key/value object in the database namespace. Its omap
entries contain:

* ``key``: serialized index key.
* Datum keys mapped to serialized timestamps and ``FieldLocation`` values.
* ``axis.<name>`` markers and per-axis key/value objects used for axis
  enumeration.

Field payloads are RADOS objects in the same database namespace. Their names
are generated from the field key and a unique timestamp/host/process value
hashed with MD5, for example::

   <field-key>.<unique-digest>.data

Multipart object writes
----------------------

The RADOS multipart handle used by FDB is an eckit abstraction over several
ordinary RADOS objects. It is not the multipart-upload protocol of an S3
gateway. It allows one logical FDB field object to be split into independently
stored RADOS objects when the payload is larger than the configured part size.

For a logical object named ``<object>``, eckit uses this naming convention:

* the first part is ``<object>``;
* subsequent parts are ``<object>;part-1``, ``<object>;part-2``, and so on.

The writer keeps the current part open until it reaches ``maxPartSize``. A
write that crosses a part boundary is divided between the current part and
the next one. FDB supplies ``rados.maxPartSize`` to the writer; the value is
in bytes. When it is zero, eckit uses the Ceph cluster's maximum object size.

On flush, eckit stores attributes on the base object describing the logical
object, including its total ``length``, number of ``parts``, and ``maxsize``.
The field location recorded by FDB refers to the logical base-object URI and
contains a byte offset and length. It does not expose the individual part
names to the catalogue.

On read, eckit reads those attributes, opens the base object followed by its
``;part-N`` objects, and presents them as one contiguous, seekable stream.
This means a field can be retrieved normally even when its bytes span several
RADOS objects. The stored offset and length still allow FDB to retrieve only
the field range within a collocated logical object.

The base object and all of its parts must be managed together. FDB therefore
uses eckit's ``ensureAllDestroyed()`` operation when removing a field and
ignores names containing ``;part-`` during object enumeration and full-wipe
discovery. A part must not be deleted independently, or the logical object
will be incomplete.

Catalogue operation
===================

Creation and reopening
----------------------

When a ``RadosCatalogueWriter`` is created from an FDB key:

#. The matching RADOS space is selected.
#. The root namespace and database namespace are opened.
#. ``main_kv`` is created if necessary.
#. A new ``catalogue_kv`` is created when the database does not yet exist.
#. The configured schema and serialized database key are stored in the
   catalogue KV.
#. The catalogue URI is registered in ``main_kv`` under the database
   namespace.

When opened from a ``rados:`` URI, the database key and schema are read from
the catalogue KV. A missing ``key`` entry is reported as a database-not-found
error.

Indexing and archiving metadata
-------------------------------

Selecting an index creates or reopens the corresponding index KV. Archiving a
datum stores its serialized field location under the datum key and updates
axis values for newly observed values. Index enumeration reads the index
references from the catalogue KV and reconstructs the RADOS indexes.

The backend intentionally does not require sorted index enumeration; the
``sorted`` argument to ``indexes()`` is ignored because RADOS key enumeration
is used directly.

Catalogue features
------------------

Implemented catalogue behavior includes:

* schema loading and persistence;
* index creation, selection, lookup, and enumeration;
* axis value persistence;
* hiding contents through persisted control entries;
* URI ownership and existence checks;
* catalogue-driven wipe and cleanup.

The catalogue's purge, move, mount, and overlay operations are not
implemented. Statistics visitors and catalogue purge/move visitors are also
unavailable for this backend.

Store operation
===============

Writing fields
--------------

``RadosStore::archive()`` obtains one generated RADOS object per FDB key and
reuses it for subsequent writes for that key during the store lifetime. It
obtains an eckit multipart write handle, writes the field bytes, and returns
an ``RadosFieldLocation`` containing the object URI, byte offset, and length.

``flush()`` flushes all open data handles. ``close()`` closes them. The
catalogue subsequently stores the returned field locations in its index KVs.

The generated field objects allow multiple writer instances to archive
concurrently to the same database. A single ``RadosStore`` instance is not
thread-safe: calls to ``archive``, ``flush``, and ``close`` must be serialized
by the caller.

Reading fields
--------------

A field location points directly to a RADOS object and byte range. The store's
retrieve path returns the field's data handle, allowing FDB to read the stored
payload using the location recorded in the index.

Store URIs use the form::

   rados:<pool>/<database-namespace>

Field object URIs add the object name as a third component. The backend checks
the URI scheme, pool, and database namespace before treating a URI as
belonging to a store.

Store features
--------------

Implemented store behavior includes:

* archive and retrieve;
* flush and close;
* object and namespace existence checks;
* listing collocated field objects;
* removing individual objects or a database namespace;
* catalogue-aware and full wipes;
* detection and removal of unrecognised objects during a full wipe;
* statistics through the normal FDB store interfaces where supported by the
  caller.

The store does not expose auxiliary URIs; ``getAuxiliaryURIs()`` returns an
empty set of results.

Use of RADOS features
=====================

The backend relies on the following Ceph/eckit RADOS features:

* **Pools** provide the physical Ceph storage boundary selected by FDB space
  placement.
* **Namespaces** isolate each FDB database within a pool. Multiple FDB
  spaces may share a pool if their root namespaces and namespace prefixes are
  distinct.
* **RADOS objects** hold field payloads and provide object existence, deletion,
  enumeration, and URI addressing.
* **Object-map key/value entries** provide compact catalogue and index metadata
  without creating a separate object for every metadata property.
* **Multipart writes** allow large field payloads to be written in parts,
  controlled by ``rados.maxPartSize``.
* **RADOS object listing** supports collocated-data discovery and detection of
  unrecognised objects during wipes.
* **URI addressing** permits stores and catalogues to be reopened from
  persisted RADOS locations.

The eckit RADOS API also provides asynchronous handles and range-read handles,
but the current FDB implementation uses synchronous data-handle operations for
retrieval and multipart write handles for archival. It does not currently use
the asynchronous or range-read APIs directly.

Wipe and cleanup safety
=======================

A catalogue-driven wipe first determines which index and data URIs are
included and which are safe. It removes only the selected catalogue entries,
index/axis KVs, and field objects. When the complete database is selected, the
database namespace and its root registry entry are removed after the contents
have been removed.

A full store wipe scans only the database namespace. Objects named as
multipart parts are handled with their main object and are not independently
treated as data records. If the namespace also contains a catalogue, the
catalogue owns the namespace cleanup and the store avoids deleting it during
an unsafe full wipe.

Limitations and operational requirements
=========================================

* Ceph and eckit RADOS support must be present at configure time.
* The target pool must exist and the configured Ceph identity must have
  permissions to access the pool and namespaces.
* RADOS placement requires at least one matching ``spaces[]`` entry and
  exactly one root in that entry.
* Operations on one ``RadosStore`` instance must be serialized by the caller.
* Separate writer instances can archive concurrently, but concurrent writers
  targeting the same index entry use last-write-wins semantics for that entry.
* Catalogue-side purge, move, mount, and overlay operations are not
  implemented.
* The RADOS backend does not persist masking metadata; wipe removes entries
  directly.
* The ``sorted`` index enumeration request is ignored.
* Auxiliary store URIs are not provided.
* Runtime tests require a reachable Ceph cluster and an existing test pool
  unless ``RADOS_TESTS_MANAGE_POOLS`` is enabled.

Minimal test setup
==================

With an existing Ceph pool:

.. code-block:: console

   cmake <bundle-source> \
     -DENABLE_RADOS=ON \
     -DENABLE_RADOSFDB=ON \
     -DFDB_RADOS_TEST_POOL=fdb_test

   ctest -R 'fdb_test_rados_(store|catalogue)'

The RADOS test environment must also provide the Ceph configuration and
credentials expected by eckit, for example through the standard Ceph
configuration directory and the configured RADOS cluster/user settings.
