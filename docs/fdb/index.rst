.. _FDB_Introduction:

FDB
===================================

:Version: |version|

FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for
storing, indexing and retrieving GRIB data. Each GRIB message is stored as a
field and indexed trough semantic metadata (i.e., physical variables such as
temperature, pressure, …). A set of fields can be retrieved specifying a
request using a specific language developed for accessing MARS Archive.

.. index:: Contents

.. toctree::
   :maxdepth: 2
   :caption: Contents

   content/requirements
   content/installation
   content/reference
   content/tools
   content/config-schema
   content/license
   cli_tools/index
   cpp_api
   c_api
   content/genindex

.. |Licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/ecmwf/fdb/blob/develop/LICENSE
   :alt: Apache Licence
