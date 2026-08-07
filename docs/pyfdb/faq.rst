FAQ
===

.. _faq-fdb5-not-found:

``ModuleNotFoundError: unable to find fdb5``
--------------------------------------------

.. code-block:: text

   raise ModuleNotFoundError(
   ModuleNotFoundError: unable to find fdb5

``PyFDB`` uses `findlibs <https://github.com/ecmwf/findlibs>`__ to locate
``libfdb5`` at import time.  This error means ``findlibs`` exhausted all of its
search paths without finding the library.

**Set** ``FDB5_HOME`` **to the root of your FDB installation** (the directory
that contains a ``lib/`` subdirectory with ``libfdb5.so`` / ``libfdb5.dylib``):

.. code-block:: sh

   export FDB5_HOME=<path-to-fdb-install-or-build>

For a local stack build this is the ``build`` directory:

.. code-block:: sh

   export FDB5_HOME=<path-to-stack>/build

If you are on a system where ``libfdb5`` is installed via a package manager or
module system and ``findlibs`` still cannot find it, also set:

.. code-block:: sh

   export FINDLIBS_DISABLE_PACKAGE=yes   # prevent stale conda/pip shadowing

You can verify what ``findlibs`` resolves after setting the variable:

.. code-block:: sh

   python -m pyfdb --print-home

See :ref:`pyfdb-dev-setup` for the full environment variable reference.


``RuntimeError: Cannot find the ecCodes library``
--------------------------------------------------

.. code-block:: text

   raise RuntimeError("Cannot find the ecCodes library")
   RuntimeError: Cannot find the ecCodes library

This error is raised by the ``eccodes`` Python package when it cannot locate
``libeccodes`` at import time.  Set ``ECCODES_HOME`` to the root of your
ecCodes installation (the directory that contains a ``lib/`` subdirectory with
``libeccodes.so`` / ``libeccodes.dylib``):

.. code-block:: sh

   export ECCODES_HOME=<path-to-eccodes-install-or-build>

For a local stack build where ecCodes was compiled as part of the bundle, this
is the same ``build`` directory as ``FDB5_HOME``:

.. code-block:: sh

   export ECCODES_HOME=<path-to-stack>/build

See :ref:`pyfdb-dev-setup` for the full environment variable reference.


``pytest`` aborts immediately (``abort`` / signal 6)
-----------------------------------------------------

.. code-block:: text

   $ pytest
   [1]    50372 abort      pytest

A hard abort at collection time — before any test output — usually means a
shared library was loaded twice or in an incompatible combination, causing an
assertion failure or fatal error deep inside the C++ runtime.

The most common cause when working with a local build is that ``findlibs``
picks up a ``libfdb5`` (or one of its dependencies) that was installed as a
**Python package** (e.g. via ``pip install fdb5lib``) instead of the locally
built one.  Two different versions of the same library loaded in the same
process will corrupt each other's state.

**Fix: disable package-based search and point directly at the local build:**

.. code-block:: sh

   export FINDLIBS_DISABLE_PACKAGE=yes
   export FDB5_HOME=<path-to-stack>/build
   export ECCODES_HOME=<path-to-stack>/build

Then run ``pytest`` again.  Use ``--print-home-deps`` to confirm which
libraries are resolved before running the suite:

.. code-block:: sh

   python -m pyfdb --print-home-deps

See :ref:`pyfdb-dev-setup` for the full environment variable reference.
Tests fail after switching Python versions
------------------------------------------

The ``pyfdb_bindings`` extension is compiled against a specific Python version
and ABI.  The CMake wheel build records the target Python at configure time and
does not automatically recompile when the active interpreter changes.  If you
switch Python versions (e.g. from 3.11 to 3.13) after an initial build, the
compiled ``.so`` carries the old ABI tag and importing it with the new
interpreter will fail with errors such as:

.. code-block:: text

   ImportError: <path>/pyfdb_bindings.cpython-311-...so: cannot open shared object file

or produce silent test failures caused by an ABI mismatch.

**Fix: do a full rebuild from a clean build directory.**  Delete the existing
build folder and re-run CMake with the new interpreter active in your venv:

.. code-block:: sh

   rm -rf <path-to-stack>/build
   mkdir <path-to-stack>/build && cd <path-to-stack>/build

   uv venv --python 3.13   # or whichever version you now want
   source ../.venv/bin/activate
   uv pip install build setuptools wheel pybind11 findlibs PyYAML pytest eccodes

   cmake .. -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
     -DENABLE_PYTHON_FDB_INTERFACE=ON -DENABLE_FDB_DOCUMENTATION=OFF -DENABLE_MEMFS=ON
   ninja

Then reinstall the editable package from the freshly built staging area:

.. code-block:: sh

   uv pip install -e <path-to-stack>/build/pyfdb-python-package-staging
