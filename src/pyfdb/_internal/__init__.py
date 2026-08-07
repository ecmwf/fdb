# (C) Copyright 2025- ECMWF.
# .
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import warnings
import findlibs

# libfdb5.so and dependencies have to be loaded prior to importing pyfdb
findlibs.load("fdb5")

from pyfdb._internal.pyfdb_internal import (
    ConfigMapper,
    FDBToolRequest,
)
from pyfdb_bindings.pyfdb_bindings import (
    FDB as _FDB,
)
from pyfdb_bindings.pyfdb_bindings import (
    URI as _URI,
)
from pyfdb_bindings.pyfdb_bindings import (
    Config,
    ControlElement,
    IndexAxis,
    ListElement,
    StatsElement,
    WipeElement,
    WipeElementType,
    PurgeElement,
    init_bindings,
    version_info,
    __fdb5_build_version__ as _fdb5_build_version,
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


_check_fdb5_version_compatibility(_fdb5_build_version, version_info())

__all__ = [
    "init_bindings",
    "version_info",
    "_DataHandle",
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
    "WipeElement",
    "WipeElementType",
    "PurgeElement",
    "IndexAxis",
    "InternalMarsSelection",
    "MarsSelection",
    "UserInputMapper",
]
