Building the Rust Crates
========================

.. code-block:: bash

   cargo add ecmwf-fdb metkit eckit
   cargo build

This builds the FDB, metkit, eckit and ecCodes from source as part of
``cargo build`` (the ``vendored`` feature, on by default). It needs Rust
1.90 or later, a C++17 compiler, CMake, git and Python 3. The first build
also needs
network access, to download ecbuild and the releases matching the crate
versions. When the crate comes from a checkout of the fdb repository, as a
path or git dependency, the C++ sources of that checkout are compiled
instead.

To link an installed FDB, 5.10.0 or later, switch the crates to the
``system`` feature and point CMake at the installations with ``FDB_DIR``,
``METKIT_DIR``, ``ECKIT_DIR`` and ``ECCODES_DIR`` or ``CMAKE_PREFIX_PATH``:

.. code-block:: bash

   cargo add ecmwf-fdb --no-default-features --features system,grib,tocfdb,fdb-remote
   cargo add metkit --no-default-features --features system,grib,bufr
   cargo add eckit --no-default-features --features system
   FDB_DIR=/path/to/fdb/install cargo build

The installed ecCodes must be built with ``ENABLE_ECCODES_THREADS=ON``.

Use ``system`` when the program must share one installation with other
software, for example on the HPC or in a bundle build and ``vendored`` for a
self-contained build.

The shared libraries of a vendored build are found at run time through
rpath entries on the binary. An application adds them from its build
script:

.. code-block:: toml

   [build-dependencies]
   bindman-utils = "0.2"

.. code-block:: rust

   // build.rs
   fn main() {
       bindman_utils::emit_rpaths();
   }

Running the Examples
--------------------

The programs quoted in :doc:`usage` are examples of the ``ecmwf-fdb``
crate, in ``rust/crates/ecmwf-fdb/examples``. From a checkout of the
repository:

.. code-block:: bash

   cd rust
   cargo run -p ecmwf-fdb --example quickstart
   cargo run -p ecmwf-fdb --example list

Cargo Features
--------------

Besides ``vendored`` and ``system``, the ``ecmwf-fdb`` crate has ``grib``,
``tocfdb``, ``fdb-remote`` and ``memfs``, all on by default: GRIB support,
the file-system TOC backend a local FDB writes to, the client of a remote
FDB and the ecCodes definition files embedded in ``libeccodes``, so a binary
needs no ``eccodes_resources`` directory next to it.

``ecmwf-fdb-sys`` exposes the remaining CMake options of the FDB as
features, so a vendored build can be configured from ``Cargo.toml``. A
feature switches on the ``ENABLE_*`` option of the same name.

.. list-table::
   :widths: 25 15 60
   :header-rows: 1

   * - Feature
     - Default
     - Effect
   * - ``radosfdb``
     - off
     - The Ceph RADOS store; needs RADOS
   * - ``lustre``
     - off
     - Lustre file striping; needs the Lustre API
   * - ``daosfdb``
     - off
     - The DAOS store; needs DAOS
   * - ``daos-admin``
     - off
     - DAOS pool management; needs DAOS
   * - ``dummy-daos``
     - off
     - DAOS emulated on the file system
   * - ``experimental``
     - off
     - Experimental upstream features
   * - ``sandbox``
     - off
     - Sandbox builds

To switch one on, add ``ecmwf-fdb-sys`` as a dependency next to
``ecmwf-fdb`` and enable the feature there:

.. code-block:: bash

   cargo add ecmwf-fdb
   cargo add ecmwf-fdb-sys --features radosfdb
