.. _FDB_Introduction:

FDB
===================================

:Version: |version|

FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for
storing, indexing and retrieving GRIB data. Each GRIB message is stored as a
field and indexed trough semantic metadata (i.e. physical variables such as
temperature, pressure, …). A set of fields can be retrieved specifying a
request using a specific language developed for accessing MARS Archive.

.. index:: Contents

.. toctree::
   :maxdepth: 2
   :caption: Contents

   content/introduction
   content/installation
   content/requirements
   content/reference
   content/tools
   content/config-schema
   content/license
   cli_tools/index
   cpp_api
   c_api
   genindex
