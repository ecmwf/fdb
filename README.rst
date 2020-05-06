===
fdb
===

|Licence|

FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for storing, indexing and retrieving GRIB data. Each GRIB message is stored as a field and indexed trough semantic metadata (i.e. physical variables such as temperature, pressure, ...).
A set of fields can be retrieved specifying a request using a specific language developed for accessing MARS_ Archive

FDB exposes a C++ API as well as CLI tools_. 


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
   make install



.. |Licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/ecmwf/fdb/blob/develop/LICENSE
   :alt: Apache Licence

.. _mars: docs/mars.rst
.. _tools: docs/tools.rst
