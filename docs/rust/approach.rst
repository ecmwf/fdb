Approach
========

Every call ends up in the same ``fdb5::FDB`` that the command-line tools and
pyfdb use, so schemas, configuration files and the on-disk format are the
same in every language.

The interface is built the same way as the eckit and metkit Rust interfaces,
and reuses their pieces: requests are ``metkit::MarsRequest``, retrieved data
is an ``eckit::DataHandle``, configuration is an ``eckit::Config``, and the
error type and logging bridge come from ``eckit``. That shared design is
described in the Rust Interface section of the eckit documentation, kept
under ``docs/content/rust`` in the `eckit repository
<https://github.com/ecmwf/eckit>`__. This page covers what is specific to the
FDB.

Two crates
----------

.. mermaid::

   flowchart TD
       app["Rust application"]
       fdb["fdb<br/>safe API"]
       sys["fdb-sys<br/>cxx bridge, exception handler, build script"]
       shim["C++ shims<br/>namespace fdb_bridge"]
       cpp["FDB C++ library"]
       eckit["eckit-sys"]
       metkit["metkit-sys"]
       eccodes["eccodes-sys"]
       other["gribjump-sys and other -sys crates<br/>for packages built on the FDB"]

       app --> fdb
       fdb --> sys
       other --> sys
       sys --> shim
       shim --> cpp
       sys --> eckit
       sys --> metkit
       sys --> eccodes

``fdb-sys`` (low level)
   Owns everything that touches C++:

   * a build script that compiles the FDB sources with ecbuild, or finds an
     installed FDB, and emits the link directives. It builds against the
     eckit, metkit and ecCodes that the ``-sys`` crates of those packages
     built or found, read from the ``DEP_*_SYS_ROOT`` variables they export,
     so one copy of each library ends up in the program;
   * the ``cxx`` bridge in ``src/lib.rs``, declaring the functions Rust may
     call. Results that are plain data, such as a list element or an axis,
     cross the bridge as shared structs by value; the ``ControlAction`` and
     ``ControlIdentifier`` enums are the C++ enums of ``fdb5``, with the
     values checked at compile time;
   * thin C++ wrapper classes in ``rust/crates/fdb-sys/cpp`` that adapt FDB
     signatures to the types ``cxx`` can pass;
   * the exception handler generated from the eckit and metkit exception
     headers;
   * a `bindman <https://github.com/ecmwf/bindman>`__ annotation on the bridge
     naming ``fdb5/api/FDB.h``. The build script compares the ``FDB`` class
     against the bridge and reports a method that is added to or removed
     from the C++ API, so the five methods left out on purpose are listed in
     the annotation.

``fdb`` (high level)
   Contains no C++. It wraps the ``fdb-sys`` types in an API that follows
   Rust conventions: fallible calls return ``fdb::Result<T>``, the C++
   iterators are Rust iterators, flags are fields of option structs, and the
   handle can be shared between threads.

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Wrapper
     - Wraps
   * - ``FdbHandle``
     - ``fdb5::FDB``
   * - ``ListIteratorHandle``, ``DumpIteratorHandle``,
       ``StatusIteratorHandle``, ``WipeIteratorHandle``,
       ``PurgeIteratorHandle``, ``StatsIteratorHandle``,
       ``ControlIteratorHandle``
     - The ``fdb5::APIIterator`` instances the C++ methods return
   * - ``MessageArchiverWrapper``
     - ``fdb5::MessageArchiver``
   * - ``Library``
     - ``fdb5_version_str``, ``fdb5_git_sha1`` and the initialisation of
       ``eckit::Main``

Rust owns a wrapper through ``cxx::UniquePtr``, so the C++ object is destroyed
when the Rust value is dropped.

One handle, many threads
------------------------

``fdb5::FDB`` documents itself as thread-safe. The Rust ``Fdb`` still keeps
the C++ object behind a ``parking_lot::Mutex`` and takes the lock for the
duration of every call, which is what makes ``Fdb`` ``Send + Sync`` without
any ``unsafe`` reasoning about the C++ internals. An ``Arc<Fdb>`` can be
shared between threads and used from all of them.

The iterators a call returns are snapshots: the C++ ``APIIterator`` holds the
results, not a reference to the handle. They are ``Send``, and consuming them
does not hold the lock, so a thread can list while another archives.

Calls block the calling thread, including the time spent in the C++ library
on I/O. In an async program, run them on a blocking thread, for example with
``tokio::task::spawn_blocking``.

Requests
--------

Queries take ``&metkit::MarsRequest<S>`` in either expansion state, because a
partial request is the normal case: any key the request leaves out matches
every value. The C++ query methods take an ``FDBToolRequest``; the bridge
builds one from the request, with the ``all`` flag set when the request is
empty and no minimum key set, so an empty request matches every database.

``retrieve`` does not expand the request. Pass canonical values, which
``str::parse`` and ``MarsRequest::expand`` produce.

Flags that the C++ methods take as positional ``bool`` arguments are fields of
option structs with the defaults of the command-line tools: ``ListOptions``
is full depth with masked entries hidden, as ``fdb-list``; ``WipeOptions``
and ``PurgeOptions`` are a dry run, as ``fdb-wipe`` and ``fdb-purge`` without
``--doit``; ``DumpOptions`` is the verbose format of ``fdb-dump``. A
destructive call therefore names its flag:

.. code-block:: rust

   fdb.wipe(&request, WipeOptions { doit: true, ..WipeOptions::default() })?

Errors
------

``fdb-sys`` declares no exception classes of its own. Its generated handler
is built from the exception headers that ``eckit-sys`` and ``metkit-sys``
publish, so an eckit or metkit exception thrown inside the FDB reaches Rust
as the same typed variant it has in those crates. The FDB's own classes, such
as ``fdb5::DatabaseNotFoundException`` or ``fdb5::RemoteFDBException``, are
subclasses of ``eckit::Exception`` without a variant of their own and arrive
as ``eckit::Error::Other`` with their message.

The high-level crate adds the errors that Rust code raises:

.. code-block:: rust

   pub enum Error {
       Eckit(eckit::Error),              // anything thrown by C++
       Io(std::io::Error),               // from a Rust reader or writer
       SizeOverflow(TryFromIntError),    // a C++ size that does not fit usize
   }

``Error`` implements ``std::error::Error`` and ``Display``, so it works with
``?``, ``Box<dyn Error>`` and error-reporting crates. Matching on the eckit
variants needs the ``eckit`` crate as a direct dependency.

Shared eckit and metkit types
-----------------------------

The FDB's C++ API is written in terms of eckit and metkit objects, and the
Rust API keeps them as those crates' types:

* ``retrieve``, ``read_uri``, ``read_uris`` and ``read_from_list`` return an
  ``eckit::DataHandle<Closed>``. Opening it gives ``std::io::Read`` and
  ``Seek``, and ``eckit::MessageReader`` iterates the fields in it.
* ``archive_reader`` and ``MessageArchiver::archive`` go the other way: a
  Rust ``Read`` is wrapped in an ``eckit::DataHandle`` on the C++ side, and
  the FDB reads from it as it would from a file, splitting the stream into
  GRIB messages with the splitter metkit registers.
* ``Fdb::open`` takes ``eckit::Config``, built from a file, from YAML text
  or key by key; ``UserConfig`` converts into one.
* Every request is a ``metkit::MarsRequest`` lent across the bridge through
  ``as_sys``.

Downstream, ``fdb-sys`` declares ``links = "fdb_sys"`` and exports to the
build scripts of crates that depend on it:

.. list-table::
   :widths: 40 60
   :header-rows: 1

   * - Variable
     - Content
   * - ``DEP_FDB_SYS_ROOT``
     - Install prefix of the FDB that was built or found
   * - ``DEP_FDB_SYS_INCLUDE``
     - The FDB include directory
   * - ``DEP_FDB_SYS_SYSTEM_FDB5_LIB``, ``DEP_FDB_SYS_SYSTEM_ECKIT_LIB``,
       ``DEP_FDB_SYS_SYSTEM_METKIT_LIB``, ``DEP_FDB_SYS_SYSTEM_ECCODES_LIB``
     - Library directories of a ``system`` build, for the rpaths of
       dependants

``gribjump-sys`` reads these to compile against the same FDB.

Logging
-------

The FDB writes diagnostics through ``eckit::Log``. ``eckit::init()`` routes
those channels to the Rust ``log`` crate; call it before opening an FDB.
Opening one when ``eckit::Main`` is not yet initialised sets it up with the
C++ defaults instead, which log to the standard streams.

Extending the bindings
----------------------

To expose another part of the FDB:

#. Add a wrapper class, or a method on ``FdbHandle``, in
   ``rust/crates/fdb-sys/cpp``, in the ``fdb_bridge`` namespace. Include
   ``fdb_exceptions.h`` first in the ``.cc`` file so exceptions are
   translated.
#. Declare it in the ``cxx`` bridge in ``rust/crates/fdb-sys/src/lib.rs``.
   Methods that can throw return ``Result``. If the method was in the
   ``ignore`` list of the ``track_cpp_api`` annotation, remove it there.
#. List new sources in ``rust/crates/fdb-sys/build.rs``.
#. Add the safe API to ``rust/crates/fdb``, converting errors with ``?`` and
   wrapping iterators in the pattern of ``ListIterator``.
