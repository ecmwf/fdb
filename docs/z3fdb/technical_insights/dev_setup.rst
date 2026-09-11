.. SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
.. SPDX-License-Identifier: Apache-2.0

.. _z3fdb_dev_setup:

Developer Setup
===============

This page describes how to build the full ECMWF stack from source using an
ecbuild bundle. This is the recommended setup for contributors who need to
work across eckit, metkit, eccodes, fdb, and gribjump together.

Bundle Layout
-------------

An ecbuild bundle is a thin top-level CMakeLists.txt that ties several
ECMWF projects together into one unified build tree. Each project is cloned
as a sibling directory and cmake cross-references them without a separate
install step in between.

A minimal bundle for Z3FDB development looks like this:

.. code-block:: cmake

   cmake_minimum_required( VERSION 3.15 FATAL_ERROR )

   find_package( ecbuild 3.4 REQUIRED HINTS $ENV{HOME}/.local/ecbuild )

   project( ecmwf_stack_bundle VERSION 0.0.1 LANGUAGES CXX )

   set( CMAKE_CXX_STANDARD 17 )
   set( CMAKE_CXX_STANDARD_REQUIRED ON )
   set( CMAKE_EXPORT_COMPILE_COMMANDS ON )

   ecbuild_bundle_initialize()

   ecbuild_bundle( PROJECT eckit    GIT "ssh://git@github.com/ecmwf/eckit"    BRANCH develop MANUAL )
   ecbuild_bundle( PROJECT eccodes  GIT "ssh://git@github.com/ecmwf/eccodes"  BRANCH develop MANUAL )
   ecbuild_bundle( PROJECT metkit   GIT "ssh://git@github.com/ecmwf/metkit"   BRANCH develop MANUAL )
   ecbuild_bundle( PROJECT fdb      GIT "ssh://git@github.com/ecmwf/fdb"      BRANCH develop MANUAL )
   ecbuild_bundle( PROJECT gribjump GIT "ssh://git@github.com/ecmwf/gribjump" BRANCH develop MANUAL )

   ecbuild_bundle_finalize()

Place this file in a ``bundle`` directory alongside the cloned source trees.
cmake needs a virtual environment with the ``build`` package present to invoke
the Python wheel builder at the end of the build. ``pybind11-stubgen`` is also
required. The build generates ``.pyi`` stub files for the C++ extension during
the cmake build. The following block can be pasted directly into a shell:

.. code-block:: bash

   python -m venv .venv
   source .venv/bin/activate
   pip install build pybind11-stubgen

   mkdir -p build && cd build
   cmake .. \
       -GNinja \
       -DCMAKE_BUILD_TYPE=RelWithDebInfo \
       -DENABLE_PYTHON_ZARR_INTERFACE=ON \
       -DENABLE_PYTHON_FDB_INTERFACE=ON \
       -DENABLE_MEMFS=ON

   export FDB5_HOME=$(pwd)
   export ECCODES_HOME=$(pwd)
   export ECCODES_PYTHON_USE_FINDLIBS=1
   export FINDLIBS_DISABLE_PACKAGE=yes

   ninja

.. note::

   The ``MANUAL`` keyword tells ecbuild not to update the clone automatically
   on every build. Each project directory is managed independently with
   ``git pull`` or ``git checkout`` as needed.

Environment Variables
---------------------

After the build completes, several environment variables are needed so that
Python can find the freshly built libraries and staging packages without
interfering with anything installed system-wide.

``FDB5_HOME``
   Root of the build tree. ``findlibs`` appends ``/lib`` to this path when
   searching for ``libfdb5``:

   .. code-block:: bash

      export FDB5_HOME=/path/to/build

``ECCODES_HOME``
   Root of the eccodes build tree. Used by both ``findlibs`` and the eccodes
   Python bindings to locate ``libeccodes``:

   .. code-block:: bash

      export ECCODES_HOME=/path/to/build

``ECCODES_PYTHON_USE_FINDLIBS``
   Tells the eccodes Python package to use ``findlibs`` for library discovery
   rather than its own search heuristics. Always set this to ``1`` when
   building from source:

   .. code-block:: bash

      export ECCODES_PYTHON_USE_FINDLIBS=1

``FINDLIBS_DISABLE_PACKAGE``
   By default `findlibs <https://github.com/ecmwf/findlibs>`_ tries to locate
   a library inside an installed Python package of the same name (e.g.
   ``eccodeslib``, ``eckitlib``). When building from source you want the
   freshly compiled libraries, not those bundled in any such package. Setting
   this variable disables that search path:

   .. code-block:: bash

      export FINDLIBS_DISABLE_PACKAGE=yes

``PYTHONPATH``
   Adds the cmake staging directories and the fdb source tree so Python
   resolves ``z3fdb``, ``pyfdb``, and ``pychunked_data_view`` from the build
   rather than from any installed wheel:

   .. code-block:: bash

      export PYTHONPATH=/path/to/build/pyfdb-python-package-staging:\
      /path/to/build/z3fdb-python-package-staging:\
      /path/to/fdb/src

A complete development shell might look like this:

.. code-block:: bash

   BUILD=/path/to/build
   FDB_SRC=/path/to/fdb

   export FDB5_HOME=${BUILD}
   export ECCODES_HOME=${BUILD}
   export ECCODES_PYTHON_USE_FINDLIBS=1
   export FINDLIBS_DISABLE_PACKAGE=yes
   export PYTHONPATH=${BUILD}/pyfdb-python-package-staging:${BUILD}/z3fdb-python-package-staging:${FDB_SRC}/src

Running the Tests
-----------------

The tests live in the ``tests`` directory inside the ``fdb`` source folder,
which sits next to the ``build`` directory in the bundle layout. With the
environment variables set as above, install the Python test dependencies and
run both suites from there:

.. code-block:: bash

   pip install -r ../fdb/requirements.txt
   cd ../fdb/tests
   pytest z3fdb pychunked_data_view -v

FAQ
---

**ImportError: cannot import name 'FDB' from 'pyfdb' (unknown location)**

   .. code-block:: text

      ImportError while loading conftest '.../fdb/tests/conftest.py'.
      tests/conftest.py:20: in <module>
          from pyfdb import FDB
      E   ImportError: cannot import name 'FDB' from 'pyfdb' (unknown location)

   This happens when ``PYTHONPATH`` is not set. Python finds a stale or
   incomplete ``pyfdb`` installation instead of the one from the build staging
   area. Set the environment variables from the section above before running
   pytest.

**ModuleNotFoundError: unable to find fdb5**

   .. code-block:: text

      raise ModuleNotFoundError(
      E   ModuleNotFoundError: unable to find fdb5

   ``findlibs`` cannot locate ``libfdb5`` because ``FDB5_HOME`` is not set.
   Point it at the build directory so ``findlibs`` can find the library under
   ``${FDB5_HOME}/lib``:

   .. code-block:: bash

      export FDB5_HOME=/path/to/build

**pytest crashes with** ``abort``**: wrong binaries picked up**

   .. code-block:: text

      [1]    39253 abort      pytest z3fdb

   A hard crash (SIGABRT or similar) usually means that a library loaded at
   runtime does not match the one it was compiled against. For example, a
   system-installed ``libfdb5`` or ``libeccodes`` is found instead of the one
   from the build tree.

   The ``pyfdb`` CLI ships a ``--print-home-deps`` flag that prints the resolved path
   for each dependency. Run it to see which library files are actually loaded:

   .. code-block:: bash

      python -m pyfdb --print-home-deps

   Check each path against the build tree. If any path points somewhere
   unexpected, verify that:

   * ``FDB5_HOME`` and ``ECCODES_HOME`` both point to the build directory.
   * ``FINDLIBS_DISABLE_PACKAGE=yes`` is exported, so ``findlibs`` does not
     fall back to a pre-built Python package such as ``eccodeslib`` or
     ``eckitlib``.

**RuntimeError: Cannot find the ecCodes library**

   .. code-block:: text

      raise RuntimeError("Cannot find the ecCodes library")
      E   RuntimeError: Cannot find the ecCodes library

   ``ECCODES_HOME`` is not set. Point it at the build directory so the eccodes
   Python bindings can locate ``libeccodes``:

   .. code-block:: bash

      export ECCODES_HOME=/path/to/build
