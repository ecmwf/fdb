# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

# libfdb5.so and ependencies have to be loaded prior to importing
# pychunked_data_view
import findlibs

findlibs.load("fdb5")

from chunked_data_view_bindings.chunked_data_view_bindings import (
    init_bindings,
    AxisDefinition,
    ChunkedDataView,
    ChunkedDataViewBuilder,
    ExtractorType,
)

# Init the bindings (eckit initialization)
init_bindings()

__all__ = [
    "init_bindings",
    "AxisDefinition",
    "ChunkedDataView",
    "ChunkedDataViewBuilder",
    "ExtractorType",
]
