# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import warnings

import pyfdb.bindings as _bindings

from pyfdb._internal.pyfdb_internal import (
    ConfigMapper,
    FDBToolRequest,
)

# Raw C++ wrapper types — internal only, not part of the public pyfdb API.
_FDB = _bindings.FDB
_URI = _bindings.URI
_DataHandle = _bindings.DataHandle
_ControlAction = _bindings.ControlAction
_ControlIdentifier = _bindings.ControlIdentifier

# Types forwarded to Python wrapper classes in pyfdb_iterator / pyfdb_type.
Config = _bindings.Config
ControlElement = _bindings.ControlElement
IndexAxis = _bindings.IndexAxis
ListElement = _bindings.ListElement
PurgeElement = _bindings.PurgeElement
StatsElement = _bindings.StatsElement
WipeElement = _bindings.WipeElement
WipeElementType = _bindings.WipeElementType

_fdb5_build_version = _bindings.__fdb5_build_version__
_version_info = _bindings.version_info


def _check_fdb5_version_compatibility(build_version, runtime_info):
    matches = [version for name, version, _, _ in runtime_info if name == "fdb"]
    runtime_version = matches[0] if matches else None
    if runtime_version is None:
        raise RuntimeError(
            "pyfdb could not determine the version of the loaded libfdb5. "
            "The library may not have loaded correctly. "
            "Run 'python -m pyfdb --print-home-deps' to inspect the dependency setup."
        )
    if runtime_version != build_version:
        warnings.warn(
            f"pyfdb was built against fdb5 {build_version} but the loaded "
            f"libfdb5 is version {runtime_version}. "
            "Behaviour may be unexpected. "
            "Run 'python -m pyfdb --print-home-deps' to inspect which libraries "
            "were picked up, or consult the pyfdb documentation.",
            UserWarning,
            stacklevel=2,
        )


_check_fdb5_version_compatibility(_fdb5_build_version, _bindings.version_info())
