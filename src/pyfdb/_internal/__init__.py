# (C) Copyright 2025- ECMWF.
# .
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

# libfdb5.so and dependencies have to be loaded prior to importing
# pyfdb

from typing import Dict
from pyfdb_bindings import pyfdb_bindings as bindings
from pyfdb_bindings.pyfdb_bindings import (
    FDB as _FDB,
    Config,
    ControlAction as _ControlAction,
    ControlIdentifier as _ControlIdentifier,
    FDBToolRequest,
    FileCopy,
    IndexAxis,
    ControlElement,
    ListElement,
    StatsElement,
    init_bindings,
)
from pyfdb_bindings.pyfdb_bindings import (
    URI as _URI,
    DataHandle as _DataHandle,
)

from .pyfdb_internal import MarsRequest as MarsRequest
from .pyfdb_internal import _flatten_values, ConfigMapper

InternalMarsSelection = Dict[str, str]

__all__ = [
    MarsRequest,
    init_bindings,
    _DataHandle,
    _flatten_values,
    _URI,
    _FDB,
    Config,
    ConfigMapper,
    FDBToolRequest,
    _ControlAction,
    _ControlIdentifier,
    ListElement,
    StatsElement,
    ControlElement,
    IndexAxis,
    FileCopy,
    InternalMarsSelection,
]
