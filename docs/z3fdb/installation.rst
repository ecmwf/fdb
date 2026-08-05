.. _z3fdb_installation:

Installation
============

Z3FDB is distributed via PyPI. Install it together with the FDB Python client:

.. code-block:: bash

   pip install z3fdb pyfdb

Verify the installation by importing the package in a Python REPL:

.. code-block:: python

   import z3fdb
   print(z3fdb)


Running the Tests
-----------------

The test suite lives in the FDB source tree rather than the installed wheel,
so you need the source at the same version as your installation. Start by
checking what version you have:

.. code-block:: bash

   pip show z3fdb | grep Version
   # Version: 5.22.0.3

Clone the repository and check out the tag corresponding to the first three
version components (drop the trailing ``.3``):

.. code-block:: bash

   git clone https://github.com/ecmwf/fdb.git
   cd fdb
   git checkout 5.22.0

Install the test dependencies, then switch into the ``tests`` directory and
run both suites together:

.. code-block:: bash

   pip install eccodes pytest
   cd tests
   pytest z3fdb pychunked_data_view -v
