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
- numpy
- pytest
- pytest-asyncio
- eccodes
- build
- setuptools
- GitPython
- sphinx
- sphinxcontrib-mermaid
- pydata-sphinx-theme
- Sphinx
- sphinx-autoapi
- sphinx-tabs

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

    find_package( ecbuild 3.4 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} $ENV{HOME}/.local/ecbuild)

    project( ecmwf_stack_bundle VERSION 0.0.1 LANGUAGES CXX)

    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

    ecbuild_bundle_initialize()

    ecbuild_bundle( PROJECT eckit       GIT "ssh://git@github.com/ecmwf/eckit"        BRANCH develop    MANUAL )
    ecbuild_bundle( PROJECT eccodes     GIT "ssh://git@github.com/ecmwf/eccodes"      TAG 2.39.2        UPDATE )
    ecbuild_bundle( PROJECT metkit      GIT "ssh://git@github.com/ecmwf/metkit"       BRANCH develop    UPDATE )
    ecbuild_bundle( PROJECT fdb         GIT "ssh://git@github.com/ecmwf/fdb"          BRANCH develop    MANUAL )
    ecbuild_bundle( PROJECT gribjump    GIT "ssh://git@github.com/ecmwf/gribjump"     BRANCH develop    MANUAL )

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

**This is currently not available**
