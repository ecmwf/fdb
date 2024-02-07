Build & Installation
--------------------
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

