.. _e2e_tests:

E2E Tests
=========

The ``tests/fdb_e2e/`` directory contains the end-to-end (e2e) test suite
for FDB. It exercises the FDB command-line tools and the ``pyfdb`` Python API
against a real FDB installation, covering write, read, list, purge, wipe,
hide, overlay, and further operations across multiple FDB configuration styles
and subtoc variants.

.. contents:: On this page
   :local:
   :depth: 2

----

Quick Start
-----------

.. code-block:: sh

   # 1. Run all e2e tests via CTest (no Python setup needed)
   cd /path/to/stack/build
   ctest -L fdb_tool_tests --output-on-failure

   # --- or, run directly with pytest ---

   # 1. Point findlibs at the build tree
   export FINDLIBS_DISABLE_PACKAGE=yes
   export ECCODES_HOME=/path/to/stack/build
   export FDB5_HOME=/path/to/stack/build

   # 2. Set up the Python environment
   uv venv && source .venv/bin/activate
   uv pip install -r tests/fdb_e2e/requirements.txt
   uv pip install -e /path/to/stack/build/pyfdb-python-package-staging

   # 3. Run all tests
   PATH=/path/to/stack/build/bin:$PATH pytest tests/fdb_e2e/

----

Prerequisites
-------------

Compiled FDB
^^^^^^^^^^^^

FDB must be compiled with the **pyfdb Python interface enabled**
(``-DENABLE_PYTHON_FDB_INTERFACE=ON`` in the CMake invocation). The test fixtures use
``pyfdb`` to initialise and validate each FDB environment before the shell
scripts run, and several tests also use it to populate the FDB with data
directly from Python. Without the pyfdb interface the fixture setup will fail.

The FDB executables (``fdb-write``, ``fdb-read``, ``fdb-list``, etc.) must be
on ``PATH`` and the shared library ``fdb5lib`` must be
loadable by ``pyfdb`` via `findlibs <https://github.com/ecmwf/findlibs>`_.

When building from a local stack (i.e. not using python packages),
set ``FINDLIBS_DISABLE_PACKAGE=yes`` so that ``findlibs`` uses only the
directory variables below and does not attempt to locate libraries through the
system package manager. Point both ``ECCODES_HOME`` and ``FDB5_HOME`` at the
CMake build directory:

.. code-block:: sh

   export FINDLIBS_DISABLE_PACKAGE=yes
   export ECCODES_HOME=/path/to/stack/build
   export FDB5_HOME=/path/to/stack/build

Python Environment
^^^^^^^^^^^^^^^^^^

Create a virtual environment, install the test dependencies, and then install
``pyfdb`` from the staged Python package that the CMake build generates. The
staged package reflects the exact version of the compiled FDB and must be
installed as an editable install so that the native extension (``_pyfdb.so``)
is loaded directly from the build tree:

.. code-block:: sh

   python -m venv .venv
   source .venv/bin/activate

   # Install test dependencies (eccodes, findlibs, GitPython, pytest, PyYAML)
   pip install -r tests/fdb_e2e/requirements.txt

   # Install pyfdb from the build-generated staging directory
   uv pip install -e /path/to/stack/build/pyfdb-python-package-staging

.. note::

   ``pyfdb-python-package-staging`` is created by the CMake build of FDB.
   Using ``uv pip install -e`` (editable mode) means the installed package
   points directly into the build tree, so there is no need to reinstall after
   a rebuild -- the updated shared library is picked up automatically.
   Omit the ``-e`` flag if you prefer a conventional install, but you will need
   to reinstall whenever the C++ library is rebuilt.

----

Running the Tests
-----------------

Via CTest (recommended)
^^^^^^^^^^^^^^^^^^^^^^^^

CTest is configured during the CMake build and is the canonical way to run
the e2e tests in CI. After building FDB:

.. code-block:: sh

   # All e2e tool tests
   ctest -L fdb_tool_tests --output-on-failure

   # One operation across all config strategies
   ctest -L fdb_tool_tests_write --output-on-failure

   # One config strategy across all operations
   ctest -L fdb_tool_tests_simple --output-on-failure

   # One specific test (naming pattern: test_fdb_tools_<config>_<operation>)
   ctest -R test_fdb_tools_fdb_tools_simple_write --output-on-failure

Via pytest (development)
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sh

   # All tests
   PATH=<build-dir>/bin:$PATH pytest tests/fdb_e2e/ -v

   # Tool tests only (shell-script based)
   PATH=<build-dir>/bin:$PATH pytest tests/fdb_e2e/tool_tests/ -v

Forwarding Extra Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``--env KEY=VALUE`` flag (repeatable) injects variables into all shell
scripts. It is useful for overriding library search paths or FDB configuration
without modifying fixtures:

.. code-block:: sh

   pytest tests/fdb_e2e/ \
       --env LD_LIBRARY_PATH=/path/to/fdb/install/lib \
       --env ECCODES_DIR=/path/to/eccodes/install

Variables supplied this way take precedence over fixture defaults.

----

Extended Test Suite
-------------------

By default the CMake build registers CTest cases only for the two most common
configuration styles, ``simple`` and ``yaml``. The remaining five styles
(``files``, ``json``, ``yaml_mars_disks``, ``yaml_tools``, ``json_tools``) are
considered extended coverage and are disabled by default to keep the standard
build fast.

To enable the full matrix, pass ``-DENABLE_PYTHON_EXTENDED_FDB_TOOL_TESTS=ON``
when configuring the build:

.. code-block:: sh

   ecbuild -DENABLE_PYTHON_FDB_INTERFACE=ON \
           -DENABLE_PYTHON_EXTENDED_FDB_TOOL_TESTS=ON \
           -- /path/to/fdb/source

.. note::

   ``ENABLE_PYTHON_EXTENDED_FDB_TOOL_TESTS`` requires
   ``ENABLE_PYTHON_FDB_INTERFACE=ON``. The extended option is silently ignored
   if the Python interface is not built.

Config styles covered by each setting:

.. list-table::
   :widths: 40 60
   :header-rows: 1

   * - CMake setting
     - Config markers active in CTest
   * - default (OFF)
     - ``simple``, ``yaml``
   * - ``-DENABLE_PYTHON_EXTENDED_FDB_TOOL_TESTS=ON``
     - ``simple``, ``yaml``, ``files``, ``json``, ``yaml_mars_disks``,
       ``yaml_tools``, ``json_tools``

When running pytest directly, all config styles are always available regardless
of this flag -- use ``-m`` to select the ones you want:

.. code-block:: sh

   # Equivalent to the extended CTest suite
   pytest tests/fdb_e2e/ -m "simple or yaml or files or json or yaml_mars_disks or yaml_tools or json_tools"

----

Selecting Tests with Markers
-----------------------------

Tests are tagged with two independent sets of markers: the FDB configuration
style and the operation being exercised. Any combination of ``-m`` expressions
can be used to select a slice of the matrix.

Configuration Strategy Markers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Marker
     - Description
   * - ``simple``
     - ``config.yaml`` at ``FDB_HOME`` root; ``FDB5_CONFIG_FILE`` set explicitly.
   * - ``files``
     - Old-style flat files in ``etc/fdb/{spaces,roots}``; no YAML config.
   * - ``yaml``
     - ``config.yaml`` in ``etc/fdb/``; only ``FDB_HOME`` needed.
   * - ``json``
     - ``config.json`` in ``etc/fdb/``; only ``FDB_HOME`` needed.
   * - ``yaml_mars_disks``
     - ``WeightedRandom`` space backed by an ``etc/disks/fdb`` disk-list file.
   * - ``yaml_tools``
     - One YAML config file per FDB tool name in ``etc/fdb/``.
   * - ``json_tools``
     - One JSON config file per FDB tool name in ``etc/fdb/``.

Operation Markers
^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Marker
     - Operation tested
   * - ``write``
     - ``fdb-write`` archival, include/exclude filters.
   * - ``read``
     - ``fdb-read`` retrieval, date ranges, step ranges, quantiles.
   * - ``list``
     - ``fdb-list`` enumeration of archived fields.
   * - ``info``
     - ``fdb-info`` / version information.
   * - ``hide``
     - ``fdb-hide`` suppression of entries from listing and retrieval.
   * - ``purge``
     - ``fdb-purge`` removal of hidden entries.
   * - ``wipe``
     - ``fdb-wipe`` deletion of FDB subtrees.
   * - ``root``
     - ``fdb-root`` root management.
   * - ``grib2fdb``
     - ``grib2fdb5`` legacy ingestion tool.
   * - ``overlay``
     - ``fdb-overlay`` layering of FDB spaces.

Examples
^^^^^^^^

.. code-block:: sh

   # Run all write tests
   pytest tests/fdb_e2e/ -m write

   # Run all tests for the 'simple' config strategy
   pytest tests/fdb_e2e/ -m simple

   # Run write tests for the 'simple' strategy only
   pytest tests/fdb_e2e/ -m "simple and write"

   # Run everything except overlay tests
   pytest tests/fdb_e2e/ -m "not overlay"

----

Understanding Test Case Names
------------------------------

Each parametrised test case is identified by a name of the form::

   test_write[simple::simple.sh]
   test_write[simple_subtocs::simple.sh]
   test_list[yaml::include_filter.sh]

The part before ``::`` is the **FDB environment spec ID** (see `Configuration
Strategy Markers`_). Specs that include subtoc mode encode it in their name:

.. list-table::
   :widths: 30 15 20 15 20
   :header-rows: 1

   * - Spec ID
     - Config style
     - Subtocs
     - Expver handler
     - Marker
   * - ``simple``
     - simple
     - no
     - no
     - ``simple``
   * - ``simple_expver``
     - simple
     - no
     - yes
     - ``simple``
   * - ``simple_subtocs``
     - simple
     - yes
     - no
     - ``simple``
   * - ``simple_subtocs_expver``
     - simple
     - yes
     - yes
     - ``simple``
   * - ``files``
     - files
     - no
     - no
     - ``files``
   * - ``files_expver``
     - files
     - no
     - yes
     - ``files``
   * - ``files_subtocs``
     - files
     - yes
     - no
     - ``files``
   * - ``files_subtocs_expver``
     - files
     - yes
     - yes
     - ``files``
   * - ``yaml``
     - yaml
     - no
     - no
     - ``yaml``
   * - ``json``
     - json
     - no
     - no
     - ``json``
   * - ``yaml_mars_disks``
     - mars_disks
     - no
     - no
     - ``yaml_mars_disks``
   * - ``yaml_tools``
     - yaml_tools
     - no
     - no
     - ``yaml_tools``
   * - ``json_tools``
     - json_tools
     - no
     - no
     - ``json_tools``

The part after ``::`` is the **shell script filename** (e.g. ``simple.sh``,
``include_filter.sh``, ``porcelain.sh``).

To rerun a single failing case by its full test ID:

.. code-block:: sh

   pytest "tests/fdb_e2e/tool_tests/test_fdb_tools.py::test_write[simple::simple.sh]" -v

----

Directory Structure
-------------------

.. code-block:: text

   tests/fdb_e2e/
   +-- conftest.py            # FdbEnvSpec, FdbEnvironment, ALL_LOCAL_SPECS, fixtures
   +-- requirements.txt
   `-- tool_tests/
       +-- conftest.py        # pytest_generate_tests -- paired (spec, script) params
       +-- test_fdb_tools.py  # one test function per FDB operation
       +-- no_subtocs/        # shell scripts for non-subtoc environments
       |   +-- write/
       |   |   +-- simple.sh
       |   |   +-- include_filter.sh
       |   |   `-- ...
       |   +-- read/
       |   `-- ...
       `-- subtocs/           # shell scripts for subtoc-enabled environments
           +-- write/
           |   +-- simple.sh
           |   `-- ...
           +-- read/
           `-- ...

Shell scripts in ``no_subtocs/`` run against environments where
``FDB5_SUB_TOCS`` is unset; those in ``subtocs/`` run against environments
where it is set to ``1``. Pytest automatically pairs each script with the
matching set of FDB environment specs so no cross-mode combinations are ever
collected.

----

How the Parametrisation Works
-------------------------------

``conftest.py`` (top-level) declares all FDB configurations as a plain list of
:class:`FdbEnvSpec` objects (``ALL_LOCAL_SPECS``). Each spec carries an ID,
a config style, subtoc mode, expver-handler mode, and pytest marks.

``tool_tests/conftest.py`` drives collection via ``pytest_generate_tests``:
for each test function that accepts an ``{operation}_script`` fixture parameter
it scans the matching shell script directory, then calls
``metafunc.parametrize(["fdb_env", script_param], pairs, indirect=["fdb_env"])``
where every entry in ``pairs`` is a ``pytest.param(spec, script, id=...)``
combining exactly the right spec with a compatible script. This avoids the
full cartesian product and the associated runtime skips.

The ``fdb_env`` fixture is *indirect*: it receives the :class:`FdbEnvSpec` as
``request.param`` and calls ``_build_local_environment`` to write config files,
instantiate a ``pyfdb.FDB`` handle, and assemble the subprocess environment
dict. The result is an :class:`FdbEnvironment` with:

- ``fdb`` -- a ``pyfdb.FDB`` handle for Python-API tests
- ``env`` -- the ``dict[str, str]`` passed to every shell script subprocess
- ``tmp`` -- the per-test temporary directory (also the script working directory)
- ``spec`` -- the originating :class:`FdbEnvSpec`

----

Adding a New Shell Script Test
--------------------------------

No Python changes are needed. Drop a ``.sh`` file into the appropriate
directory and pytest picks it up automatically at the next collection:

.. code-block:: sh

   # New no-subtoc write variant
   vim tests/fdb_e2e/tool_tests/no_subtocs/write/my_new_scenario.sh

   # New subtoc read variant
   vim tests/fdb_e2e/tool_tests/subtocs/read/my_new_scenario.sh

The script receives a pre-configured FDB environment through the inherited
environment variables (``FDB_HOME``, ``FDB5_CONFIG_FILE``, etc.) set by the
test fixture. The working directory is set to a fresh per-test temporary
directory, so the script can write data files and FDB content without
interfering with other tests.

**Conventions:**

- Use ``set -euxo pipefail`` at the top so the test fails fast on any error.
- Exit with ``exit 0`` at the end (the framework treats any non-zero exit as
  a test failure).
- Use ``fdb-list --all --minimum-keys="" --porcelain | tee out`` and line-count
  assertions to verify write results.

Adding a New Operation
^^^^^^^^^^^^^^^^^^^^^^

To add an entirely new operation (e.g. ``fdb-compare``):

1. Create ``tests/fdb_e2e/tool_tests/no_subtocs/compare/`` (and
   ``subtocs/compare/`` if subtoc-specific behaviour differs) and add ``.sh``
   scripts.
2. Add ``"compare"`` to ``_CASES`` in ``tool_tests/conftest.py``.
3. Add a test function ``test_compare(fdb_env, compare_script)`` to
   ``test_fdb_tools.py``.
4. Register the ``pytest.mark.compare`` marker in ``pyproject.toml`` (or
   ``setup.cfg``) and in ``tool_tests/CMakeLists.txt``.

----

Known Expected Failures
------------------------

``overlay/wipe.sh`` is marked ``xfail`` in both the ``no_subtocs/`` and
``subtocs/`` variants pending issue **FDB-652**. Once the issue is resolved,
remove the ``xfail`` annotation from the overlay block in
``tool_tests/conftest.py``.

----

Changelog
---------

- ``domain=g`` added to ``fdb-overlay`` and ``fdb-hide`` invocations.
- ``no_subtocs/read/steprange.sh`` -- MARS request adjusted to match updated
  schema.
- ``subtocs/wipe/duplicate_data.sh`` -- updated to match current wipe behaviour.
- ``no_subtocs/wipe/porcelain.sh`` -- updated to match current output format.
- Standalone ``config_yaml`` setup dropped; its coverage is equivalent to the
  ``simple`` setup (both write a ``config.yaml``).
