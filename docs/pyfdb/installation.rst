.. _installation-label:
Installation
############

Requirements
************

Build Dependencies
^^^^^^^^^^^^^^^^^^^^

+----------+---------------------------------------------+
|Dependency|Link                                         |
+----------+---------------------------------------------+
|CMake     |http://www.cmake.org/                        |
+----------+---------------------------------------------+
|ecbuild   |https://github.com/ecmwf/ecbuild             |
+----------+---------------------------------------------+
|Pybind11  |https://pybind11.readthedocs.io              |
+----------+---------------------------------------------+

Runtime Dependencies
^^^^^^^^^^^^^^^^^^^^

+----------+---------------------------------------------+
|Dependency|Link                                         |
+----------+---------------------------------------------+
|eccodes   |https://github.com/ecmwf/eccodes             |
+----------+---------------------------------------------+
|eckit     |https://github.com/ecmwf/eckit               |
+----------+---------------------------------------------+
|metkit    |https://github.com/ecmwf/mekit               |
+----------+---------------------------------------------+

Python Dependencies
^^^^^^^^^^^^^^^^^^^
.. code-block:: sh

    numpy
    pytest
    pytest-asyncio
    eccodes
    build
    setuptools
    GitPython
    sphinx
    sphinxcontrib-mermaid
    pydata-sphinx-theme
    Sphinx
    sphinx-autoapi
    sphinx-tabs

Build from sources (recommended way):
*************************************

To install ``PyFDB`` from the sources you first need to create a directory which will contain our bundle file.
A bundle file represents a subset of our stack; at least the dependencies we need to build ``PyFDB``.

Create a folder ``stack`` and switch to it:

.. code-block:: sh

   mkdir stack && cd stack

Place the following ``CMakeLists.txt`` in it

.. code-block:: cmake

    cmake_minimum_required( VERSION 3.18 FATAL_ERROR )

    find_package( ecbuild 3.8 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} $ENV{HOME}/.local/ecbuild)

    project( ecmwf_stack_bundle VERSION 0.0.1 LANGUAGES CXX)

    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

    ecbuild_bundle_initialize()

    ecbuild_bundle( PROJECT eckit       GIT "ssh://git@github.com/ecmwf/eckit"        BRANCH develop    UPDATE )
    ecbuild_bundle( PROJECT eccodes     GIT "ssh://git@github.com/ecmwf/eccodes"      BRANCH develop    UPDATE )
    ecbuild_bundle( PROJECT metkit      GIT "ssh://git@github.com/ecmwf/metkit"       BRANCH develop    UPDATE )
    ecbuild_bundle( PROJECT fdb         GIT "ssh://git@github.com/ecmwf/fdb"          BRANCH develop    UPDATE )
    ecbuild_bundle( PROJECT gribjump    GIT "ssh://git@github.com/ecmwf/gribjump"     BRANCH develop    UPDATE )

    ecbuild_bundle_finalize()

In the ``stack`` folder, create a ``build`` folder

.. code-block:: sh
   
   mkdir build && cd build

Run the following ``cmake`` command to configure the stack with its dependencies:

.. code-block:: sh

   cmake -DCMAKE_INSTALL_PREFIX=../install \
         -DCMAKE_BUILD_TYPE=RelWithDebInfo \
         .. -G Ninja \ 
         -DENABLE_MEMFS=ON \
         -DENABLE_PYTHON_FDB_INTERFACE=ON \
         -DENABLE_FDB_DOCUMENTATION=OFF
    
.. tip::

    The ``cmake`` variables can be changed accordingly. Use at least the 
    ``-DENABLE_PYTHON_FDB_INTERFACE`` to build PyFDB.

    You can also switch to make by dropping ``-G Ninja``.

For certain functions of the ``PyFDB`` we need an active python venv. If you use ``uv``, you can create
and activate a ``venv`` in the root of this bundle (where the ``CMakeLists.txt`` is located):

.. code-block:: sh

   uv venv
   source .venv/bin/activate
   uv pip install -r fdb/requirements.txt
   # Add those requirements if you want to build the docs locally
   uv pip install -r fdb/docs/fdb/requirements.txt

After configuration with ``cmake`` run ``ninja`` in the created ``build`` folder:

.. code-block:: sh

   ninja

If the command exists successfully, adjust your ``PYTHONPATH`` to:

.. code-block:: sh

   export PYTHONPATH=<path-to-build-folder>/pyfdb-python-package-staging:<path-to-stack-folder>/fdb/src


Afterwards, verify whether the tests are successfully running, by switching to the test folder of PyFDB
and execute ``pytest``:

.. code-block:: sh

   cd <path-to-stack-folder>/fdb/tests/pyfdb
   pytest



Installation via PyPI
*********************

Install the package from pypi in your `venv`:

.. code-block:: sh

   uv venv
   source .venv/bin/activate
   uv pip install pyfdb

Set the `FDB_HOME` environment variable accordingly:

.. code-block:: sh

    export FDB_HOME=<path_to_fdb_home>

Diagnosing Library Resolution
******************************

``PyFDB`` uses `findlibs <https://github.com/ecmwf/findlibs>`__ to locate the ``fdb5``
shared library and its runtime dependencies at import time. If you encounter errors caused
by the wrong library version being loaded, the built-in CLI can help you inspect what
``findlibs`` resolves on your system.

Print the installation root of the ``fdb5`` library:

.. code-block:: sh

    python -m pyfdb --print-home

Print the resolved home directories for all runtime dependencies (``fdb5``, ``eckit``,
``metkit``, ``eccodes``), together with any active ``FINDLIBS_DISABLE_*`` environment
variables that suppress specific search paths:

.. code-block:: sh

    python -m pyfdb --print-home-deps

Example output:

.. code-block:: text

    λ python -m pyfdb --print-home-deps
    2026-07-01 18:24:07 | INFO   | Findlibs Environment:
    2026-07-01 18:24:07 | INFO   |     FINDLIBS_DISABLE_PACKAGE: yes
    2026-07-01 18:24:07 | INFO   |     FINDLIBS_DISABLE_PYTHON: yes
    2026-07-01 18:24:07 | INFO   | Findlibs Lookup
    2026-07-01 18:24:07 | ERROR  |     eckit: not found by findlibs
    2026-07-01 18:24:07 | INFO   |     eccodes [Optional]: /path/to/install
    2026-07-01 18:24:07 | ERROR  |     metkit: not found by findlibs
    2026-07-01 18:24:07 | INFO   |     fdb5: /path/to/install
    2026-07-01 18:24:07 | INFO   | Dependency Versions:
    2026-07-01 18:24:07 | INFO   |     eccodes 2.48.0 (bd5b4ca0) /path/to/install/lib/libeccodes.dylib
    2026-07-01 18:24:07 | INFO   |     eckit 2.0.7 (9daf0377) /path/to/install/lib/libeckit.dylib
    2026-07-01 18:24:07 | INFO   |     eckit_geo 2.0.7 (9daf0377) /path/to/install/lib/libeckit_geo.dylib
    2026-07-01 18:24:07 | INFO   |     eckit_spec 2.0.7 (9daf0377) /path/to/install/lib/libeckit_spec.dylib
    2026-07-01 18:24:07 | INFO   |     fdb 5.21.4 (9da45883) /path/to/install/lib/libfdb5.dylib
    2026-07-01 18:24:07 | INFO   |     gribjump 0.10.4 (fc510969) /path/to/install/lib/libgribjump.dylib
    2026-07-01 18:24:07 | INFO   |     metkit 1.18.1 (cbfcd750) /path/to/install/lib/libmetkit.dylib

In this example, ``eckit`` and ``metkit`` were not found by ``findlibs`` because
``FINDLIBS_DISABLE_PACKAGE`` and ``FINDLIBS_DISABLE_PYTHON`` suppressed those search paths,
yet the libraries were still loaded and their versions reported via the system linker.
``ERROR`` lines indicate dependencies ``findlibs`` could not locate — set the corresponding
``<LIBNAME>_HOME`` environment variable to resolve them explicitly.

If a dependency is resolved from an unexpected location, consult
the `findlibs documentation <https://github.com/ecmwf/findlibs>`__ for the full list of
supported variables and search-path precedence rules.

