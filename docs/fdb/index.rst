.. _FDB_Introduction:

FDB
===

:Version: |version|

The FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for storing,
indexing and retrieving meteorological data. The data objects to be stored should be
self-describing (messages), and are stored and indexed according to their semantic
metadata (such as 'atmospheric level' or 'physical parameter'). A set of messages can
be retrieved specifying a request using a specific language developed for accessing
the MARS Archive.

.. index:: Contents

.. toctree::
   :maxdepth: 2
   :caption: Contents

   content/introduction
   content/mars
   content/config-schema
   content/environment-variables
   cli_tools/index
   content/api
   content/license
   genindex

.. toctree::
   :maxdepth: 2
   :caption: Development

   e2e_tests/index

.. |Licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/ecmwf/fdb/blob/develop/LICENSE
   :alt: Apache Licence
