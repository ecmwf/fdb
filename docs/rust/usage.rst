Using the ``ecmwf-fdb`` Crate
=============================

The code on this page is quoted from the examples of the ``ecmwf-fdb``
crate; :ref:`rust/building:Running the Examples` says how to run them. The
methods of each type are listed on `docs.rs <https://docs.rs/ecmwf-fdb>`_.

Runtime Initialisation
----------------------

Call ``eckit::init()`` before any other call. The FDB's log output then goes
through the ``log`` crate under the target ``fdb``, so an ``env_logger``
filter of ``RUST_LOG=fdb=debug`` switches on its debug channel.

Opening an FDB
--------------

``Fdb::open_default`` reads the configuration from the environment the way
the command-line tools do: YAML text in ``FDB_CONFIG``, else the file named
by ``FDB_CONFIG_FILE``, else ``etc/fdb/config.yaml`` under ``FDB_HOME``.
``FDB5_CONFIG`` and ``FDB5_CONFIG_FILE`` are read as fallbacks. ``Fdb::open``
takes an ``eckit::Config`` built from a file, from YAML text or key by key
and an optional ``UserConfig`` overlay:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/open.rs
   :language: rust
   :start-after: // [open]
   :end-before: // [end open]

A configuration for a local FDB names the schema and the root directories
to store data under. The examples on this page create one in a temporary
directory:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/common/mod.rs
   :language: rust
   :start-after: // [config]
   :end-before: // [end config]

``name`` reports the kind of FDB opened, ``local`` or ``remote``.

Archiving
---------

A field is archived under a ``Key`` holding the metadata values the schema
requires or as a GRIB message from which the FDB reads the key itself.
``archive`` takes a key and the encoded bytes, ``archive_raw`` takes GRIB
bytes alone and ``archive_reader`` streams GRIB messages from any
``std::io::Read``. Nothing is visible to readers until ``flush``:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/archive.rs
   :language: rust
   :start-after: // [archive]
   :end-before: // [end archive]
   :dedent:

``on_archive`` registers a callback the FDB invokes for each field with its
key, bytes and location; ``on_flush`` one it invokes on every flush:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/archive.rs
   :language: rust
   :start-after: // [callbacks]
   :end-before: // [end callbacks]
   :dedent:

``MessageArchiver`` is the archiving path of the MARS client: it takes a
``Read + Seek`` source, applies a key modifier to every message and returns
the number of bytes archived.

Listing
-------

Any key a ``list`` request does not set matches every value, so a request
with only ``class`` and ``expver`` lists an experiment. Each element carries the
location of the field and its key split into the three levels of the FDB
index, ``db_key``, ``index_key`` and ``datum_key``, which ``full_key``
joins:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/list.rs
   :language: rust
   :start-after: // [list]
   :end-before: // [end list]
   :dedent:

``ListOptions`` holds ``depth``, the number of index levels to traverse, and
``deduplicate``, which hides masked entries. The defaults, depth 3 and
deduplication on, are those of ``fdb-list``. With a depth below 3 the first
``next`` returns ``SeriousBug``: only datum-level elements have a location.

``dump_compact`` consumes the iterator and writes the aggregated form that
``fdb-list --compact`` prints, one line per database and index with the
datum keys folded into ranges. It returns the field and byte counts:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/list.rs
   :language: rust
   :start-after: // [compact]
   :end-before: // [end compact]
   :dedent:

``axes`` returns the values present for every key of the matching indexes,
down to the depth given as for ``list``:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/list.rs
   :language: rust
   :start-after: // [axes]
   :end-before: // [end axes]
   :dedent:

Retrieving
----------

``retrieve`` takes a request and returns an ``eckit::DataHandle`` over the
matching fields, in request order. ``eckit::MessageReader`` splits what the
open handle reads into messages. The request is used as given, so build it
with ``str::parse`` or ``MarsRequest::expand``:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/retrieve.rs
   :language: rust
   :start-after: // [retrieve]
   :end-before: // [end retrieve]
   :dedent:

When the locations are known from a listing, ``read_uris`` reads them
without consulting the index again, in storage order when the
``in_storage_order`` flag is set and in the order given otherwise;
``read_from_list`` does the same straight from the list iterator:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/retrieve.rs
   :language: rust
   :start-after: // [read]
   :end-before: // [end read]
   :dedent:

Maintenance
-----------

The remaining methods are the operations of the ``fdb-*`` tools, each
returning an iterator over the databases the request matches. ``dump``
yields the entries of each catalogue, ``status`` the location and state of
each database and ``stats_iter`` the field and duplicate counts of each
index with the report text ``fdb-stats`` prints:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/admin.rs
   :language: rust
   :start-after: // [inspect]
   :end-before: // [end inspect]
   :dedent:

``wipe`` removes the data of the matching databases and ``purge`` the masked
duplicates. Both are a dry run with the default options; ``doit: true``
performs the removal:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/admin.rs
   :language: rust
   :start-after: // [wipe]
   :end-before: // [end wipe]
   :dedent:

``control`` enables or disables the ``ControlIdentifier`` features on the
matching databases and ``enabled`` reports the state for the handle:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/admin.rs
   :language: rust
   :start-after: // [control]
   :end-before: // [end control]
   :dedent:

Threads
-------

``Fdb`` is ``Send`` and ``Sync``:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/threads.rs
   :language: rust
   :start-after: // [threads]
   :end-before: // [end threads]
   :dedent:

The iterators and ``MessageArchiver`` are ``Send`` but not ``Sync``.
``Key`` and the element structs are both.

Errors
------

``fdb::Error`` has three variants: ``Eckit`` holding an ``eckit::Error`` for
an exception thrown by the C++ libraries, ``Io`` for a failure of the writer
given to ``dump_compact`` and ``SizeOverflow`` for a size that does not fit
the platform. A reader failing inside ``archive_reader`` arrives as
``eckit::ReadError``; a configuration of unknown type is a ``SeriousBug``:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/errors.rs
   :language: rust
   :start-after: // [errors]
   :end-before: // [end errors]
   :dedent:

The ``Display`` output of an ``Eckit`` error keeps the class name, for
example ``eckit::SeriousBug: ...``; ``Other`` prints ``eckit error:`` and
the message.
