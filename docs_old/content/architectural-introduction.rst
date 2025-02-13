.. _architectural-introduction-label:

Architectural Introduction
~~~~~~~~~~~~~~~~~~~~~~~~~~

|Licence|

FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for storing, indexing and retrieving GRIB data. Each GRIB message is stored as a field and indexed trough semantic metadata (i.e. physical variables such as temperature, pressure, ...).
A set of fields can be retrieved specifying a request using a specific language developed for accessing :ref:`mars-request-label` Archive

FDB exposes a C++ API as well as CLI :ref:`tools-label`, for further information, see :ref:`technical-introduction-label`.

The overall structure of the system the FDB is operating on can be seen in:


.. toctree::
   :maxdepth: 1

   architecture/introduction
   architecture/overview


.. |Licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/ecmwf/fdb/blob/develop/LICENSE
   :alt: Apache Licence

