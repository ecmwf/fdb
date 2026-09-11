.. SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
.. SPDX-License-Identifier: Apache-2.0

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


.. _z3fdb_gribjump_availability:

Optional: the GribJump Extractor
--------------------------------

:class:`~pychunked_data_view.ExtractorType.Grib` works in every installation.
:class:`~pychunked_data_view.ExtractorType.GribJump` does not: it is compiled only when fdb is
configured with

.. code-block:: bash

   -DENABLE_ZARR_GRIBJUMP_EXTRACTOR=ON

which is **off by default**, so a wheel from PyPI will not normally have it. The feature needs a
bundle build in which gribjump is present as a sibling project. Gribjump itself depends on
fdb5, so it cannot simply be resolved as an ordinary installed dependency.

The class is importable and constructible either way, so code does not have to branch on how the
wheel was built. Only building a view from it fails:

.. code-block:: text

   RuntimeError: GribJumpExtractorDefinition: this build has no GribJump support. Rebuild fdb
   with -DENABLE_ZARR_GRIBJUMP_EXTRACTOR=ON (requires a bundle build providing gribjump).

To check before you get there:

.. code-block:: python

   from pychunked_data_view import has_gribjump_extractor

   print(has_gribjump_extractor)   # False on a default build

The two tutorials both use GribJump; everything else in this documentation works without it.

.. seealso:: :ref:`z3fdb_extractor_backends` for what the two backends do and how to choose.

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
