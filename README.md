fdb
===

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ecmwf/fdb/blob/develop/LICENSE)

FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for storing, indexing and retrieving GRIB data. Each GRIB message is stored as a field and indexed trough semantic metadata (i.e. physical variables such as temperature, pressure, ...).
A set of fields can be retrieved specifying a request using a specific language developed for accessing [MARS Archive](MARS.md)

FDB exposes a C++ API as well as CLI tools. Most common tools are the following:

- [fdb-write](FDB-TOOLS.md##fdb-write) -- Inserts data into the FDB, creating a new databases if needed.
- [fdb-list](FDB-TOOLS.md##fdb-list) -- Lists the contents of the FDB databases.
- [fdb-read](FDB-TOOLS.md##fdb-read) -- Read data from the FDB and write this data into a specified target file.

Requirements
------------

Runtime dependencies:

- eccodes -- http://github.com/ecmwf/eccodes
- eckit -- http://github.com/ecmwf/eckit
- metkit -- http://github.com/ecmwf/metkit

Build dependencies:

- CMake --- For use and installation see http://www.cmake.org/
- ecbuild --- ECMWF library of CMake macros ()

Installation
------------

fdb employs an out-of-source build/install based on CMake.

Make sure ecbuild is installed and the ecbuild executable script is found ( `which ecbuild` ).

Now proceed with installation as follows:

```bash
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
```
