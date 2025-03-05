===
fdb
===

|Licence|

FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for storing, indexing and retrieving GRIB data. Each GRIB message is stored as a field and indexed trough semantic metadata (i.e. physical variables such as temperature, pressure, ...).
A set of fields can be retrieved specifying a request using a specific language developed for accessing MARS_ Archive

FDB exposes a C++ API as well as CLI tools_. 

FDB uses a config file and schema with more infomration found here config-schema_


Requirements
============

Runtime dependencies:

:eccodes: http://github.com/ecmwf/eccodes
:eckit: http://github.com/ecmwf/eckit
:metkit: http://github.com/ecmwf/metkit


Build dependencies:

:CMake: For use and installation see http://www.cmake.org/
:ecbuild: ECMWF library of CMake macros ()

Installation
============

fdb employs an out-of-source build/install based on CMake.

Make sure ecbuild is installed and the ecbuild executable script is found ( ``which ecbuild`` ).

Now proceed with installation as follows:
::

   # Clone repo
   git clone https://github.com/ecmwf/fdb
   cd fdb

   # Environment --- Edit as needed
   srcdir=$(pwd)
   builddir=build
   installdir=$HOME/local  
   
   # 1. Create the build directory:
   mkdir $builddir
   cd $builddir

   # 2. Run CMake
   ecbuild --prefix=$installdir -- -DCMAKE_INSTALL_PREFIX=</path/to/installations> $srcdir
   
   # 3. Compile / Install
   make -j10
   ctest
   make install

How to reference FDB5
---------------------

Two publications, co-authored by Simon D. Smart, Tiago Quintino, Baudouin Raoult
describe fdb architecture and have been presented at PASC'17 `A Scalable Object Store for Meteorological and Climate Data`_ and PASC'19 `A High-Performance Distributed Object-Store for Exascale Numerical Weather Prediction and Climate`_

In the following the two BibTeX snippets:
::

   @inproceedings{10.1145/3093172.3093238,
      author    = {Smart, Simon D. and Quintino, Tiago and Raoult, Baudouin},
      title     = {A Scalable Object Store for Meteorological and Climate Data},
      year      = {2017},
      isbn      = {9781450350624},
      publisher = {Association for Computing Machinery},
      address   = {New York, NY, USA},
      url       = {https://doi.org/10.1145/3093172.3093238},
      doi       = {10.1145/3093172.3093238},
      booktitle = {Proceedings of the Platform for Advanced Scientific Computing Conference},
      articleno = {13},
      numpages  = {8},
      location  = {Lugano, Switzerland},
      series    = {PASC ’17}
   }



   @inproceedings{10.1145/3324989.3325726,
      author    = {Smart, Simon D. and Quintino, Tiago and Raoult, Baudouin},
      title     = {A High-Performance Distributed Object-Store for Exascale Numerical Weather Prediction and Climate},
      year      = {2019},
      isbn      = {9781450367707},
      publisher = {Association for Computing Machinery},
      address   = {New York, NY, USA},
      url       = {https://doi.org/10.1145/3324989.3325726},
      doi       = {10.1145/3324989.3325726},
      booktitle = {Proceedings of the Platform for Advanced Scientific Computing Conference},
      articleno = {16},
      numpages  = {11},
      location  = {Zurich, Switzerland},
      series    = {PASC ’19}
   }
  

.. _A Scalable Object Store for Meteorological and Climate Data: https://dl.acm.org/doi/pdf/10.1145/3093172.3093238
.. _A High-Performance Distributed Object-Store for Exascale Numerical Weather Prediction and Climate: https://dl.acm.org/doi/pdf/10.1145/3324989.3325726

.. |Licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/ecmwf/fdb/blob/develop/LICENSE
   :alt: Apache Licence

.. _mars: mars.rst
.. _tools: tools.rst
.. _config-schema: config-schema.rst
