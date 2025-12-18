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

from .pyfdb_type import (
    URI,
    DataHandle,
    MarsSelection,
    WildcardMarsSelection,
    ControlAction,
    ControlIdentifier,
)
from .pyfdb import FDB

from .pyfdb_iterator import (
    ListElement,
    MoveElement,
    StatusElement,
    WipeElement,
    IndexAxis,
)

__all__ = [
    FDB,
    URI,
    DataHandle,
    ListElement,
    WipeElement,
    StatusElement,
    MoveElement,
    MarsSelection,
    WildcardMarsSelection,
    ControlAction,
    ControlIdentifier,
    IndexAxis,
]
