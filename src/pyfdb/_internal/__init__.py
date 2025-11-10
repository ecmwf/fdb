# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

# libfdb5.so and dependencies have to be loaded prior to importing
# pyfdb

from .pyfdb_internal import MarsRequest as MarsRequest, _flatten_values

from pyfdb_bindings.pyfdb_bindings import (
    init_bindings,
    DataHandle as _DataHandle,
    URI as _URI,
    Config,
    FDB,
    FDBToolRequest,
    ControlAction,
    ControlIdentifier,
    ListElement,
    StatsElement,
    ControlElement,
    IndexAxis,
    FileCopy,
)

from pyfdb_bindings import pyfdb_bindings as bindings

__all__ = [
    MarsRequest,
    init_bindings,
    _DataHandle,
    _flatten_values,
    _URI,
    FDB,
    Config,
    FDBToolRequest,
    ControlAction,
    ControlIdentifier,
    ListElement,
    StatsElement,
    ControlElement,
    IndexAxis,
    FileCopy,
]
