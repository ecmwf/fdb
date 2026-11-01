Building the Rust crates
========================

The crates build with Cargo. CMake and ecbuild are driven by the build script
of ``fdb-sys`` and do not need to be run by hand.

Toolchain requirements
----------------------

* Rust 1.90 or later
* A C++17 compiler
* CMake and git
* Python 3 on ``PATH``, used by the ecCodes build to embed its definition
  files (the ``memfs`` feature, on by default)

Vendored and system builds
--------------------------

Two Cargo features select where the C++ libraries come from. They are
mutually exclusive, and the one chosen for the FDB is forwarded to
``eckit-sys``, ``metkit-sys`` and ``eccodes-sys``, so all four libraries come
from the same place.

``vendored`` (default)
   Builds the FDB from source into Cargo's output directory, using ecbuild,
   which is downloaded automatically. When the crate is used from a checkout
   of the fdb repository, by path or as a git dependency, the C++ sources of
   that same checkout are compiled, so Rust and C++ changes can be developed
   and tested together; the build prints a warning when the checkout's
   ``VERSION`` differs from the crate version. Otherwise the release tag
   matching the ``fdb-sys`` version is cloned.

``system``
   Links an installed FDB, located with CMake ``find_package(fdb5)``.
   Requires FDB 5.10.0 or later. Set ``FDB_DIR`` or ``CMAKE_PREFIX_PATH`` if
   it is not in a default location, and likewise ``ECKIT_DIR``,
   ``METKIT_DIR`` and ``ECCODES_DIR`` for its dependencies:

   .. code-block:: toml

      [dependencies]
      fdb = { version = "5.25", default-features = false, features = ["system"] }

   .. code-block:: bash

      FDB_DIR=/path/to/fdb/install cargo build

Use ``system`` when the program must share one installation with other
software, for example on the HPC or in a bundle build. Use ``vendored`` for a
self-contained build.

The shared libraries of a vendored build are found at run time through rpath
entries on the final binary. An application adds them from its build script
with ``bindman_utils::emit_rpaths()``:

.. code-block:: toml

   [build-dependencies]
   bindman-utils = "0.2"

.. code-block:: rust

   // build.rs
   fn main() {
       bindman_utils::emit_rpaths();
   }

Backend features
----------------

Which parts of the FDB a vendored build compiles is decided by the Cargo
features of ``fdb-sys``. Each switches on the CMake option of the same
name.

.. list-table::
   :widths: 25 15 60
   :header-rows: 1

   * - Feature
     - Default
     - Effect
   * - ``grib``
     - on
     - GRIB support: ``ENABLE_GRIB``, plus the GRIB product in ecCodes and
       the GRIB splitter in metkit, which ``archive_raw`` and
       ``archive_reader`` need to find the key of a message
   * - ``tocfdb``
     - on
     - The file-system TOC backend, the store a local FDB writes to
   * - ``fdb-remote``
     - on
     - The client side of a remote FDB
   * - ``memfs``
     - on
     - Embed the ecCodes definition and sample files in ``libeccodes``, so
       a binary needs no ``eccodes_resources`` directory next to it
   * - ``radosfdb``, ``lustre``, ``daosfdb``, ``daos-admin``,
       ``dummy-daos``
     - off
     - Storage backends that need RADOS, the Lustre API or DAOS installed;
       ``dummy-daos`` emulates DAOS on the file system
   * - ``experimental``, ``sandbox``
     - off
     - Experimental upstream features

The four defaults are features of the ``fdb`` crate too, forwarded to
``fdb-sys``, so turning off the ``fdb`` crate's default features drops them
along with ``vendored``. An optional backend is switched on through
``fdb-sys`` directly:

.. code-block:: toml

   [dependencies]
   fdb = "5.25"
   fdb-sys = { version = "5.25", features = ["radosfdb"] }

Run-time configuration
----------------------

``Fdb::open_default`` resolves the configuration the way the C++ library
and the command-line tools do, in this order:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Setting
     - Effect
   * - ``FDB_CONFIG``
     - YAML text holding the whole configuration
   * - ``FDB_CONFIG_FILE``
     - Path of a configuration file
   * - ``FDB_HOME``
     - Prefix under which ``etc/fdb/config.yaml`` is looked up

``FDB5_CONFIG`` and ``FDB5_CONFIG_FILE`` are read as fallbacks. ``Fdb::open``
with a configuration bypasses the lookup.

Without ``memfs``, ecCodes needs its definition files at run time, found
through ``ECCODES_DEFINITION_PATH``, or ``eccodes_resources`` next to the
binary for a packaged install.

Crate versions
--------------

Both crates carry the version of the FDB release they wrap. The version is
set once for the workspace in ``rust/Cargo.toml``, and a test in each crate
fails if it drifts from the ``VERSION`` file of the repository.

Build environment variables
---------------------------

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Variable
     - Effect
   * - ``FDB_DIR``
     - Install prefix of the FDB, used by ``system`` builds
   * - ``ECKIT_DIR``, ``METKIT_DIR``, ``ECCODES_DIR``
     - Install prefixes of the dependencies, used by their ``system`` builds
   * - ``CMAKE_PREFIX_PATH``
     - Additional CMake search paths
   * - ``DOCS_RS``
     - When set, the C++ build is skipped and only the generated error
       handler is produced, which is enough for ``cargo doc``

Working on the crates
---------------------

All commands run from the ``rust/`` directory of the repository:

.. code-block:: bash

   cargo clippy --all-targets     # lint; the workspace enables pedantic lints
   cargo fmt                      # format Rust sources
   cargo test                     # unit and integration tests
   cargo doc --no-deps --open     # API reference

The integration tests under ``rust/crates/fdb/tests`` create a local FDB in
a temporary directory with the schema in ``tests/fixtures``, archive the GRIB
fixtures and read them back, so they need no FDB installation. The workspace
also holds ``tools/fdb-hammer``, a load generator, and the ``fdb_list``,
``fdb_read`` and ``fdb_axes`` examples, which are command-line tools in the
style of ``fdb-list``, ``fdb-read`` and ``fdb-axes``.

The programs in ``docs/rust/examples`` are the code shown in these pages.
They are complete ``main`` programs and build as examples of a package that
depends on the ``fdb``, ``metkit`` and ``eckit`` crates; the ones that need
data create a temporary FDB with the module in ``examples/common``.
