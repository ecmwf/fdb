# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""PyFDB is python client to the FDB5 library. It provides list, retrieve,
and archive functions to interact with FDB5 databases.
See the docstrings of pyfdb.list, pyfdb.archive, pyfdb.retrieve for more information.

Example:

    import pyfdb
    fdb = pyfdb.FDB()

    fdb.archive(open('x138-300.grib', "rb").read(), key)
    fdb.flush()

    for entry in fdb.list(request = {...}, duplicates=False, keys=False):
        print(entry)



Locating the FDB5 library:
    PyFDB uses findlibs to locate the fdb5 shared library. findlibs will attempt to
    locate the FDB5 library by looking in the following locations:
        * sys.prefix
        * $CONDA_PREFIX
        * $FDB5_HOME or $FDB5_DIR
        * $LD_LIBRARY_PATH and $DYLD_LIBRARY_PATH
        * "/", "/usr/", "/usr/local/", "/opt/", and "/opt/homebrew/".

    You can set "$FDB5_HOME" or load a conda environment to direct pyFDB to a particular FDB5 library.
    You can check which library is being used by printing `pyfdb.lib`
    $ print(pyfdb.lib)
        <pyfdb.pyfdb.PatchedLib FDB5 version 5.12.1 from /path/to/lib/libfdb5.dylib>

    See https://github.com/ecmwf/findlibs for more info on library resolution.



See https://github.com/ecmwf/pyfdb for more information on pyfdb.
"""

from .pyfdb import *
