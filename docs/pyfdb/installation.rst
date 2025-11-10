Installation
============

Install `PyFDB` (requires Python 3.10+):

.. code-block:: bash

    python3 -m pip install --upgrade git+https://github.com/ecmwf/pyfdb.git@master

Alternative sources:

* PyPI (not yet available):
  
  .. code-block:: bash

      python3 -m pip install pyfdb


**Note:** Since eccodes 2.37.0, Python-shipped binaries may conflict; to use local install:

.. code-block:: bash

    export ECCODES_PYTHON_USE_FINDLIBS=1


