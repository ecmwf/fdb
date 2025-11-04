# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
from pathlib import Path

from z3fdb import (
    AxisDefinition,
    ExtractorType,
    SimpleStoreBuilder,
)

from z3fdb_profiler.view import load_view


def make_store(path: Path, fdb_config=None):
    view = load_view(path)
    builder = SimpleStoreBuilder(fdb_config)
    for part in view.parts:
        builder.add_part(
            part.request,
            [AxisDefinition(i.keywords, i.chunking) for i in part.axis],
            ExtractorType.GRIB,
        )
    if view.extension_axis:
        builder.extendOnAxis(view.extension_axis)
    return builder.build()
