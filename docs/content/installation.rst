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