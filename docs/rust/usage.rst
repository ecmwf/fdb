Using the ``fdb`` crate
=======================

Every fallible call in the high-level crate returns ``fdb::Result<T>``, an
alias for ``Result<T, fdb::Error>``; see :doc:`approach` for how the error
type relates to the C++ exceptions. The programs on this page are in
``docs/rust/examples`` and build as described in :doc:`building`. Call
``eckit::init()`` once, before anything else; it sets up the eckit runtime
and routes the FDB's log output to the Rust ``log`` crate.

Opening an FDB
--------------

``Fdb`` wraps ``fdb5::FDB``. ``open_default`` resolves the configuration
from the environment, as the command-line tools do; ``open`` takes an
``eckit::Config`` built from a file, from YAML text, or key by key, and an
optional ``UserConfig`` overlay with the per-instance settings
``useSubToc``, ``preloadTocBTree`` and ``limits.read``:

.. literalinclude:: examples/open.rs
   :language: rust
   :start-after: // [open]
   :end-before: // [end open]

A configuration for a local FDB names the schema and the root directories
to store data under. The examples on this page create one in a temporary
directory:

.. literalinclude:: examples/common/mod.rs
   :language: rust
   :start-after: // [config]
   :end-before: // [end config]

``name`` reports the kind of FDB that was opened, ``local`` or ``remote``,
and ``fdb::version`` the version of the linked library.

Archiving
---------

A field is archived under a ``Key``, the set of metadata values the schema
requires, or as a GRIB message from which the FDB reads the key itself.
``archive`` takes a key and the encoded bytes, ``archive_raw`` takes GRIB
bytes alone, and ``archive_reader`` streams GRIB messages from any
``std::io::Read`` without buffering them first. Nothing is visible to
readers until ``flush``:

.. literalinclude:: examples/archive.rs
   :language: rust
   :start-after: // [archive]
   :end-before: // [end archive]
   :dedent:

``on_archive`` registers a callback the FDB invokes for each field with its
key, bytes and location, and ``on_flush`` one it invokes on every flush:

.. literalinclude:: examples/archive.rs
   :language: rust
   :start-after: // [callbacks]
   :end-before: // [end callbacks]
   :dedent:

``stats`` counts the archive, location and flush operations of the handle.
``MessageArchiver`` wraps ``fdb5::MessageArchiver``, the archiving path of
the MARS client: it takes a ``Read + Seek`` source, applies a key modifier to
every message, and returns the number of bytes archived.

Listing
-------

``list`` takes a request and returns an iterator of ``ListElement``. Any key
the request does not set matches every value, so a request with only
``class`` and ``expver`` lists an experiment. Each element carries the
location of the field and its key split into the three levels of the FDB
index, ``db_key``, ``index_key`` and ``datum_key``, which ``full_key`` joins:

.. literalinclude:: examples/list.rs
   :language: rust
   :start-after: // [list]
   :end-before: // [end list]
   :dedent:

``ListOptions`` holds the two flags of the C++ ``list``: ``depth`` selects
how many index levels to traverse, and ``deduplicate`` hides masked entries.
The defaults, depth 3 and deduplication on, are those of ``fdb-list``. A
depth below 3 fails in this release: the bridge reads the field location of
every element, and only datum-level elements have one.

``dump_compact`` consumes the iterator and writes the aggregated form that
``fdb-list --compact`` prints, one line per database and index with the
datum keys folded into ranges, and returns the field and byte counts:

.. literalinclude:: examples/list.rs
   :language: rust
   :start-after: // [compact]
   :end-before: // [end compact]
   :dedent:

``axes`` returns the values present for every key of the matching indexes,
as a map from key name to values:

.. literalinclude:: examples/list.rs
   :language: rust
   :start-after: // [axes]
   :end-before: // [end axes]
   :dedent:

Retrieving
----------

``retrieve`` takes a request and returns an ``eckit::DataHandle`` over the
matching fields, in request order. The handle is returned closed; opening it
gives a reader, and ``eckit::MessageReader`` splits what it reads into
messages. The request is used as given, so build it with ``str::parse`` or
``MarsRequest::expand`` to get canonical values:

.. literalinclude:: examples/retrieve.rs
   :language: rust
   :start-after: // [retrieve]
   :end-before: // [end retrieve]
   :dedent:

When the locations are already known from a listing, ``read_uris`` reads
them without consulting the index again, in storage order or in the order
given, and ``read_from_list`` does the same straight from the list iterator:

.. literalinclude:: examples/retrieve.rs
   :language: rust
   :start-after: // [read]
   :end-before: // [end read]
   :dedent:

Inspecting and maintaining
--------------------------

The remaining methods are the administrative operations of the ``fdb-*``
tools, each returning an iterator over the databases the request matches.
``dump`` yields the entries of each catalogue, ``status`` the location and
state of each database, and ``stats_iter`` the field and duplicate counts of
each index together with the report text ``fdb-stats`` prints:

.. literalinclude:: examples/admin.rs
   :language: rust
   :start-after: // [inspect]
   :end-before: // [end inspect]
   :dedent:

``wipe`` removes the data of the matching databases and ``purge`` the masked
duplicates. Both are a dry run with the default options, reporting what
would be removed; ``doit: true`` performs the removal:

.. literalinclude:: examples/admin.rs
   :language: rust
   :start-after: // [wipe]
   :end-before: // [end wipe]
   :dedent:

``control`` enables or disables listing, retrieval, archiving or wiping on
the matching databases, and ``enabled`` reports the state for the handle:

.. literalinclude:: examples/admin.rs
   :language: rust
   :start-after: // [control]
   :end-before: // [end control]
   :dedent:

Threads
-------

``Fdb`` is ``Send + Sync``. Behind an ``Arc`` one handle serves any number of
threads; each call takes the handle's lock for its duration, and the
iterators a call returns are consumed without it:

.. literalinclude:: examples/threads.rs
   :language: rust
   :start-after: // [threads]
   :end-before: // [end threads]
   :dedent:

Calls block until the C++ library returns, including its I/O. In an async
program, run them on a blocking thread, for example with
``tokio::task::spawn_blocking``.

Errors
------

``fdb::Error`` has three variants: ``Eckit`` for an exception thrown by the
C++ libraries, ``Io`` for a failure in a Rust reader or writer, and
``SizeOverflow`` for a size that does not fit the platform:

.. literalinclude:: examples/errors.rs
   :language: rust
   :start-after: // [errors]
   :end-before: // [end errors]
   :dedent:

Naming the eckit variants needs the ``eckit`` crate as a dependency of the
application. ``Error`` implements ``std::error::Error``, so ``?`` and
``Box<dyn Error>`` work without it.
