Installation
============

The installation of FDB is only one of two steps necessary for FDB to work properly.
See the setup of config and schema to get a fully working FDB instance!

Manual Build & Installation
---------------------------

FDB employs an out-of-source build/install based on CMake.
Make sure ecbuild is installed and the ecbuild executable script is found ( ``which ecbuild`` ).

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

Bundle Build & Installation
---------------------------
You also have the option to install FDB with all its dependencies as a bundle using
`ecbundle <https://github.com/ecmwf/ecbundle>`_. Install `ecbundle` via

::

   git clone https://github.com/ecmwf/ecbundle
   export PATH=$(pwd)/ecbundle/bin:${PATH}

Afterwards you can use the `bundle.yml` below to install FDB and its dependencies.
Simply locate it in a directory. 

.. code-block:: yaml

   ---
   name    : fdb_bundle                    # The name given to the bundle
   version : 2024.2                       # A version given to the bundle
   cmake   : CMAKE_EXPORT_COMPILE_COMMANDS=ON 

   projects :

     - ecbuild :
         git     : https://github.com/ecmwf/ecbuild
         version : 3.8.2
         bundle  : false                # (do not build/install, only download)

     - eckit :
         git     : https://github.com/ecmwf/eckit
         version : 1.25.2
         require : ecbuild
         cmake   : >                    # turn off some unnecessary eckit features
                   ENABLE_ECKIT_CMD=off
                   ENABLE_ECKIT_SQL=off

     - metkit :
         git     : https://github.com/ecmwf/metkit
         version : master
         require : ecbuild 

     - eccodes:
         git     : https://github.com/ecmwf/eccodes
         version : master
         require : ecbuild 
         cmake   : >                    
                   ENABLE_AEC=ON
                   ENABLE_MEMFS=ON

     - fdb :
         git     : https://github.com/ecmwf/fdb
         version : develop
         require : ecbuild eckit metkit eccodes

   options:
     - with-aec:
         help  : Enable AEC library
         cmake : ENABLE_AEC=ON

     - with-memfs:
         help  : Enable MEMFS library
         cmake : ENABLE_MEMFS=ON

Run

::

   ecbuild create && \
   ecbundle build --install-dir=<path_to_install> -j10

to build FDB with all dependencies. A final `./build/install.sh` will install FDB
to the path you specified.


Python API
----------
There is also a Python-Interface for accessing FDB. A thin python wrapper around the existing
FDB functionality can be found here: `PyFDB <https://github.com/ecmwf/pyfdb>`_.
