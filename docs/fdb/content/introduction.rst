============
Introduction
============

|Licence|

The FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for storing,
indexing and retrieving meteorological data. The data objects to be stored should be
self-describing (messages), and are stored and indexed according to their semantic
metadata (such as 'atmospheric level' or 'physical parameter'). A set of messages can
be retrieved specifying a request using a specific language developed for accessing
the MARS Archive.

FDB exposes a C++ API as well as CLI :doc:`../cli_tools/index`.

.. toctree::
   :maxdepth: 1

   requirements
   installation
   reference


.. |Licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/ecmwf/fdb/blob/develop/LICENSE
   :alt: Apache Licence
