===============================
Welcome to FDB's documentation!
===============================

The FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for 
storing, indexing and retrieving GRIB data. Each GRIB message is stored as a 
field and indexed trough semantic metadata (i.e. physical variables such as 
temperature, pressure, ...). A set of fields can be retrieved specifying a
request using a specific language developed for accessing MARS Archive.

The documentation is divided into three parts: 

***************************************
:ref:`architectural-introduction-label`
***************************************

The aim of this document to given an overview of the system landscape, showing
how the FDB integrates into an existing setup, consisting of a data archive, using
the example of `MARS <https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation>`_.

***********************************
:ref:`technical-introduction-label`
***********************************

The aim of this part of the documentation is to give a broad and technical overview of the 
API of the FDB. 

*************************************
:ref:`operational-introduction-label`
*************************************

The aim of this part of the documentation is to give a broad ...
API of the FDB. 

This part of the documentations aims at operations and how to configure and deploy the FDB.

.. index:: Structure

.. toctree::
   :maxdepth: 2
   :caption: Structure

   content/concept/introduction
   content/concept/installation
   content/concept/architecture
   content/concept/client-reference
   content/concept/administrative-docs
   content/concept/fdb-tools
   content/concept/data-governance
   content/concept/reference-docs

.. raw:: html

   <hr>

.. toctree::
   :maxdepth: 2
   :caption: Misc

   content/reference
   content/license
