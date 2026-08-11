.. _pyfdb-dev-setup:

Development Setup
##################

This guide walks through setting up a local build of the full FDB stack so you
can iterate on ``PyFDB`` against a locally compiled ``libfdb5``.
It covers the C++ bundle build and the Python editable install that lets you
edit Python source files and have changes take effect immediately — no
reinstall required.

Prerequisites
~~~~~~~~~~~~~

The following must be available on your system before you start:

* **CMake** ≥ 3.18
* **ecbuild** ≥ 3.8
* **pybind11** ≥ 3.0.1
* **Ninja** (recommended; ``make`` also works, drop ``-G Ninja`` below)
* **uv** — `Astral's fast Python package installer <https://docs.astral.sh/uv/>`__
* A C++17-capable compiler (GCC ≥ 9, Clang ≥ 10, Apple Clang ≥ 12)

Setting up the Bundle
~~~~~~~~~~~~~~~~~~~~~

An ecbuild *bundle* is a thin ``CMakeLists.txt`` that clones and builds a set of
ECMWF projects together in a single CMake configure step.
Create a ``stack`` directory and place the following file in it:

.. code-block:: sh

   mkdir stack && cd stack

.. code-block:: cmake
   :caption: stack/CMakeLists.txt

   cmake_minimum_required( VERSION 3.18 FATAL_ERROR )

   find_package( ecbuild 3.8 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} $ENV{HOME}/.local/ecbuild)

   project( ecmwf_stack_bundle VERSION 0.0.1 LANGUAGES CXX )

   set(CMAKE_CXX_STANDARD 17)
   set(CMAKE_CXX_STANDARD_REQUIRED ON)
   set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

   ecbuild_bundle_initialize()

   ecbuild_bundle( PROJECT eckit    GIT "https://github.com/ecmwf/eckit"    BRANCH develop UPDATE )
   ecbuild_bundle( PROJECT eccodes  GIT "https://github.com/ecmwf/eccodes"  BRANCH develop UPDATE )
   ecbuild_bundle( PROJECT metkit   GIT "https://github.com/ecmwf/metkit"   BRANCH develop UPDATE )
   ecbuild_bundle( PROJECT fdb      GIT "https://github.com/ecmwf/fdb"      BRANCH develop UPDATE )

   ecbuild_bundle_finalize()

.. tip::

   To work against a local checkout of any dependency instead of having ecbuild
   clone it, replace the ``GIT`` / ``BRANCH`` arguments with
   ``SOURCE <path-to-checkout>``. This is the typical workflow when you are
   modifying ``fdb`` or one of its dependencies alongside ``PyFDB``.

Python Environment
~~~~~~~~~~~~~~~~~~

Create and activate a virtual environment in the ``stack`` root **before**
running CMake. CMake's Python finder and pybind11 will both pick up the active
interpreter, so the extension is always linked against the venv's Python:

.. code-block:: sh

   cd stack          # the directory that holds CMakeLists.txt
   uv venv --python 3.11 # or any supported version
   source .venv/bin/activate

Install the Python build toolchain and ``PyFDB``'s runtime dependencies:

.. code-block:: sh

   uv pip install build setuptools wheel pybind11 \
                  findlibs PyYAML pytest eccodes GitPython

.. note::

   The ``build`` package is needed because the CMake target ``pyfdb-wheel``
   invokes ``python -m build`` to produce the wheel from the staging area.
   It must be present in the active venv before running ``cmake`` configuration.

Configure and Build
~~~~~~~~~~~~~~~~~~~

Create a ``build`` subdirectory, configure with CMake, then compile with Ninja:

.. code-block:: sh

   mkdir build && cd build

   cmake .. \
     -G Ninja \
     -DCMAKE_BUILD_TYPE=RelWithDebInfo \
     -DCMAKE_INSTALL_PREFIX=../install \
     -DENABLE_PYTHON_FDB_INTERFACE=ON \
     -DENABLE_MEMFS=ON \
     -DENABLE_FDB_DOCUMENTATION=OFF

.. tip::

   ``-DENABLE_PYTHON_FDB_INTERFACE=ON`` is the flag that activates the
   pybind11 extension build and creates the Python staging area at
   ``build/pyfdb-python-package-staging/``.

Build the entire stack:

.. code-block:: sh

   ninja

A successful build populates ``build/pyfdb-python-package-staging/`` with the
compiled extension (``pyfdb_bindings/pyfdb_bindings.cpython-*.so``) and a
symlink to the Python source tree, making the directory a self-contained,
installable Python project.

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

Set the following variables before importing ``PyFDB`` so that ``findlibs`` can
locate the locally built shared libraries instead of searching the system or
any installed Python packages:

.. code-block:: sh

   # Tell findlibs where libfdb5.so lives (build/lib/)
   export FDB5_HOME=<path-to-stack>/build

   # Tell findlibs where libeccodes.so lives
   # If eccodes was built as part of the bundle this is the same directory
   export ECCODES_HOME=<path-to-stack>/build

   # Disable conda / pip package search so the local build is always used
   export FINDLIBS_DISABLE_PACKAGE=yes

.. note::

   If you link against a system eccodes (e.g. installed via a package manager),
   omit ``ECCODES_HOME`` and let ``findlibs`` discover it via the default search
   path.

   ``FINDLIBS_DISABLE_PACKAGE=yes`` prevents ``findlibs`` from loading a
   library out of an installed conda or pip package that may shadow the locally
   built version. The ``--print-home-deps`` CLI flag (see :doc:`installation`)
   is useful for verifying which paths are resolved after setting these
   variables.

Editable Install
~~~~~~~~~~~~~~~~

With the build complete and the environment variables exported, install
``PyFDB`` from the staging area in **editable** mode. Editable mode places a
link from the active venv back into the staging area, so any change you make to
the Python source in the repository is immediately visible without reinstalling:

.. code-block:: sh

   uv pip install -e <path-to-stack>/build/pyfdb-python-package-staging

Verify that the library is found correctly:

.. code-block:: sh

   python -m pyfdb --print-home

Running Tests
~~~~~~~~~~~~~

Switch to the ``PyFDB`` test folder and run ``pytest``:

.. code-block:: sh

   cd <path-to-stack>/fdb/tests/pyfdb
   pytest

.. note::

   Depending on your system's ``ulimit`` settings you may encounter
   ``OSError: [Errno 24] Too many open files`` caused by unreleased file
   handles during the test run. Re-run with ``--lf`` to execute only the
   last failed tests:

   .. code-block:: sh

      pytest --lf
