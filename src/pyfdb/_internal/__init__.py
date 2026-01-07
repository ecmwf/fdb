# (C) Copyright 2025- ECMWF.
# .
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

# libfdb5.so and dependencies have to be loaded prior to importing
# pyfdb

from pyfdb._internal.pyfdb_internal import (
    ConfigMapper,
    InternalMarsSelection,
    _flatten_values,
)
from pyfdb._internal.pyfdb_internal import MarsRequest as MarsRequest
from pyfdb_bindings.pyfdb_bindings import (
    FDB as _FDB,
)
from pyfdb_bindings.pyfdb_bindings import (
    URI as _URI,
)
from pyfdb_bindings.pyfdb_bindings import (
    Config,
    ControlElement,
    FDBToolRequest,
    FileCopy,
    IndexAxis,
    ListElement,
    StatsElement,
    init_bindings,
)
from pyfdb_bindings.pyfdb_bindings import (
    ControlAction as _ControlAction,
)
from pyfdb_bindings.pyfdb_bindings import (
    ControlIdentifier as _ControlIdentifier,
)
from pyfdb_bindings.pyfdb_bindings import (
    DataHandle as _DataHandle,
)

__all__ = [
    "MarsRequest",
    "init_bindings",
    "_DataHandle",
    "_flatten_values",
    "_URI",
    "_FDB",
    "Config",
    "ConfigMapper",
    "FDBToolRequest",
    "InternalMarsSelection",
    "_ControlAction",
    "_ControlIdentifier",
    "ListElement",
    "StatsElement",
    "ControlElement",
    "IndexAxis",
    "FileCopy",
    "InternalMarsSelection",
]
