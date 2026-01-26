.. pyfdb documentation master file
    :author: ECMWF
    :date: 2025-11-10

Fields DataBase - FDB documentation
===================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :hidden:

   fdb/index
   pyfdb/index
   z3fdb/index

:ref:`FDB <FDB_Introduction>` itself is part of `ECMWF <https://www.ecmwf.int>`’s
high‑performance data infrastructure: it stores each GRIB message as a field,
indexes it by meteorological metadata (e.g., `parameter`, `level`, `date/time`),
and serves recent outputs to post‑processing tasks and users. In operational
use, FDB acts as a hot cache in front of the long‑term MARS archive, enabling
fast access to newly generated data. 

:ref:`PyFDB <PyFDB_Introduction>` is the Python interface to ECMWF’s Fields DataBase (`FDB <github.com/ecmwf/fdb>`__), a
domain‑specific object store designed to efficiently archive, index, list, and
retrieve GRIB fields produced by numerical weather prediction workflows. It
provides a thin, idiomatic Python layer over the `FDB5` client library installed
on your system, so you can drive FDB operations directly from Python scripts
and notebooks. 

:ref:`Z3FDB <Z3FDB_Introduction>` is the Python-Zarr interface to ECMWF’s Fields DataBase (`FDB <github.com/ecmwf/fdb>`__).
It provides a thin, idiomatic Python layer over the `FDB5` client library installed
on your system, so you can extract Zarr data by creating a virtual Zarr store. The view is described 
via a MARS request and can be used to create a virtual Zarr store containing the data of the MARS
request. For further information, see :ref:`Z3FDB <Z3FDB_Introduction>` or visit the `Zarr project <github.com/zarr-developers/zarr-python>`__.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

