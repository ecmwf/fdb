# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

# libfdb5.so and dependencies have to be loaded prior to importing
# pyfdb
import findlibs

findlibs.load("fdb5")

from .pyfdb_type import URI, FDBToolRequest, Config, MarsRequest, DataHandle
from .pyfdb import PyFDB

from .pyfdb_iterator import (
    ListElement,
    WipeElement,
    StatusElement,
    MoveElement,
)

__all__ = [
    PyFDB,
    Config,
    MarsRequest,
    FDBToolRequest,
    URI,
    DataHandle,
    ListElement,
    WipeElement,
    StatusElement,
    MoveElement,
]
