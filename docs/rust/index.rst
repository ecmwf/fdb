.. _Rust_Introduction:

Rust Interface
==============

:Version: |version|

The Rust interface lives in the ``rust/`` directory of the fdb repository
and calls the same FDB client library as :ref:`pyfdb <PyFDB_Introduction>`.
This chapter is for developers writing applications against the crates.

``ecmwf-fdb`` is the crate applications depend on; it gives the C++ library
a Rust API, with ``Result`` instead of exceptions, iterators over query
results and a handle that threads share. ``ecmwf-fdb-sys`` builds or links
the C++ library underneath it.

.. note::

   Use ``ecmwf-fdb`` in your code. ``ecmwf-fdb-sys`` goes into a
   ``Cargo.toml`` only to switch on a storage backend of the C++ library
   (:doc:`building`); calling it directly means handling ``cxx`` pointers
   and raw exception strings yourself.

Coverage
--------

.. list-table::
   :widths: 32 33 35
   :header-rows: 1

   * - Rust
     - C++
     - Notes
   * - ``fdb::Fdb``
     - ``fdb5::FDB``
     - Open from a configuration, archive, retrieve, read, list, axes, dump,
       status, wipe, purge, stats, control, flush and the archive and flush
       callbacks
   * - ``fdb::Key``
     - ``fdb5::Key``
     - The key an explicit archive is stored under
   * - ``fdb::ListIterator``, ``DumpIterator``, ``StatusIterator``,
       ``WipeIterator``, ``PurgeIterator``, ``StatsIterator``,
       ``ControlIterator``
     - ``fdb5::ListIterator`` and the other ``APIIterator`` instances
     - Rust iterators yielding ``Result``; ``ListIterator::dump_compact`` is
       ``fdb5::ListIterator::dumpCompact``
   * - ``fdb::ListOptions``, ``DumpOptions``, ``WipeOptions``,
       ``PurgeOptions``
     - The ``bool`` and ``int`` arguments of the C++ methods
     - Named fields with the defaults of the ``fdb-list``, ``fdb-dump``,
       ``fdb-wipe`` and ``fdb-purge`` tools
   * - ``fdb::UserConfig``
     - The user configuration overlay of ``fdb5::Config``
     - ``useSubToc``, ``preloadTocBTree`` and ``limits.read``
   * - ``fdb::MessageArchiver``
     - ``fdb5::MessageArchiver``
     - The archiving path of the MARS client, with key modifiers
   * - ``fdb::ControlAction``, ``fdb::ControlIdentifier``
     - ``fdb5::ControlAction``, ``fdb5::ControlIdentifier``
     - The C++ enums
   * - ``fdb::version``, ``fdb::git_sha1``
     - ``fdb5_version_str``, ``fdb5_git_sha1``
     - Version of the linked library
   * - ``fdb::Error``
     - The ``eckit::Exception`` classes, ``std::io::Error``
     - ``Eckit`` for an exception thrown by the C++ libraries, ``Io`` and
       ``SizeOverflow`` for failures in Rust code

Quick Start
-----------

Add the crates:

.. code-block:: bash

   cargo add ecmwf-fdb metkit eckit

The crate version is the version of the FDB release it wraps. ``metkit``
provides the requests and ``eckit`` provides ``eckit::init``, the
configuration and the data handles that the FDB API takes and returns.
:doc:`building` lists the requirements and the alternative of linking
installed libraries.

The program creates an FDB in a temporary directory, archives one field
under its key, lists the experiment and retrieves the field:

.. literalinclude:: ../../rust/crates/ecmwf-fdb/examples/quickstart.rs
   :language: rust
   :start-after: // [main]
   :end-before: // [end main]

.. toctree::
   :maxdepth: 2
   :caption: Contents
   :hidden:

   approach
   usage
   building

The per-item API reference is on docs.rs: `ecmwf-fdb
<https://docs.rs/ecmwf-fdb>`_ and `ecmwf-fdb-sys
<https://docs.rs/ecmwf-fdb-sys>`_.
