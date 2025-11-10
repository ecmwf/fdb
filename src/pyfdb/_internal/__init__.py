# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

# libfdb5.so and dependencies have to be loaded prior to importing
# pyfdb

from .pyfdb_internal import MarsRequest, _flatten_values

from pyfdb_bindings.pyfdb_bindings import (
    init_bindings,
    DataHandle,
    URI,
    Config,
    FDB,
    FDBToolRequest,
    ControlAction,
    ControlIdentifier,
    ListElement,
    StatsElement,
    ControlElement,
    IndexAxis,
)

from pyfdb_bindings import pyfdb_bindings as bindings

__all__ = [
    MarsRequest,
    init_bindings,
    DataHandle,
    _flatten_values,
    URI,
    FDB,
    Config,
    FDBToolRequest,
    ControlAction,
    ControlIdentifier,
    ListElement,
    StatsElement,
    ControlElement,
    IndexAxis,
]
