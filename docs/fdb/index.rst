.. _FDB_Introduction:

Fields DataBase - FDB Documentation
====================================

:Version: |version|

The FDB is a domain-specific object store developed at ECMWF for
storing, indexing and retrieving GRIB data. Each GRIB message is stored as a
field and indexed through semantic metadata (i.e. physical variables such as
temperature, pressure, …). A set of fields can be retrieved specifying a
request using a specific language developed for accessing MARS Archive.

.. index:: Contents

.. toctree::
   :maxdepth: 2
   :caption: Contents

   content/introduction
   content/mars
   content/config-schema
   cli_tools/index
   content/api
   content/license
   genindex

.. |Licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/ecmwf/fdb/blob/develop/LICENSE
   :alt: Apache Licence
