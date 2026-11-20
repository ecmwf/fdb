Approach
========

This page gives the reasons behind the shape of the interface. How to use it
is on :doc:`usage`. The FDB API takes and returns eckit and metkit types:
requests are ``metkit::MarsRequest``, retrieved data is an
``eckit::DataHandle`` and configuration is an ``eckit::Config``. Those,
``eckit::init`` and the eckit error classes are documented in the Rust
Interface chapters of the eckit and metkit documentation.

Two Crates
----------

``ecmwf-fdb-sys``
   Unsafe bindings to the C++ library, built on `cxx <https://cxx.rs>`_.

   * :ref:`Builds libfdb5 from source or links an installed one <rust/building:Building the Rust Crates>`.
   * :ref:`Exposes the FDB CMake options as Cargo features <rust/building:Cargo Features>`.
   * Converts eckit and metkit exceptions into typed errors.

``ecmwf-fdb``
   Safe Rust API over ``ecmwf-fdb-sys``.

   * :ref:`fdb::Result and a matchable fdb::Error <rust/usage:Errors>`.
   * :ref:`One handle shared between threads <rust/approach:Handle>`.
   * :ref:`Iterators over query results <rust/usage:Listing>`.
   * :ref:`Option structs with the tools' defaults <rust/approach:Requests>`.
   * eckit data handles for :ref:`retrieved data <rust/usage:Retrieving>`
     and :ref:`archived streams <rust/usage:Archiving>`.

Handle
------

``fdb5::FDB`` is thread-safe. ``Fdb`` keeps it behind a mutex and takes the
lock for the duration of every call, so an ``Arc<Fdb>`` serves any number of
threads. The iterators a call returns are snapshots: the C++ iterator holds
the results, not a reference to the handle, so a thread can list while
another archives.

Calls block the calling thread for the time the C++ library spends on I/O.
In an async program, run them on a blocking thread, for example with
``tokio::task::spawn_blocking``.

Requests
--------

Queries take ``&MarsRequest<S>`` in either expansion state: any key the
request leaves out matches every value and an empty request matches every
database. ``retrieve`` does not expand the request.

Flags that the C++ methods take as positional ``bool`` arguments are fields
of option structs with the defaults of the command-line tools:
``ListOptions`` is full depth with masked entries hidden, as ``fdb-list``;
``WipeOptions`` and ``PurgeOptions`` are a dry run, as ``fdb-wipe`` and
``fdb-purge`` without ``--doit``; ``DumpOptions`` is the verbose format of
``fdb-dump``. A destructive call names its flag:

.. code-block:: rust

   fdb.wipe(&request, WipeOptions { doit: true, ..WipeOptions::default() })?

Error Type
----------

``fdb::Error`` nests ``eckit::Error`` instead of copying its variants, so an
eckit exception thrown inside the FDB is the same value it would be coming
from eckit itself. The FDB's own exception classes, such as
``fdb5::DatabaseNotFoundException``, have no variant and arrive as
``eckit::Error::Other`` with their message. ``RemoteFDBException`` and
``RemoteFDBUnauthorised`` derive from ``eckit::RemoteException`` and arrive
as that variant.
