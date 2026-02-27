============
Introduction
============

|Licence|

FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for storing, indexing and retrieving GRIB data. Each GRIB message is stored as a field and indexed through semantic metadata (i.e. physical variables such as temperature, pressure, ...).
A set of fields can be retrieved specifying a request using a specific language developed for accessing :doc:`mars` Archive.

FDB exposes a C++ API as well as CLI :doc:`../cli_tools/index`.

.. toctree::
   :maxdepth: 1

   requirements
   installation
   reference


.. |Licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/ecmwf/fdb/blob/develop/LICENSE
   :alt: Apache Licence
