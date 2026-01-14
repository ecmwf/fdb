# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

# libfdb5.so and ependencies have to be loaded prior to importing
# pychunked_data_view
import findlibs

findlibs.load("fdb5")

from pychunked_data_view.chunked_data_view import (
    AxisDefinition,
    ChunkedDataView,
    ChunkedDataViewBuilder,
    ExtractorType,
)

__all__ = [
    "AxisDefinition",
    "ChunkedDataView",
    "ChunkedDataViewBuilder",
    "ExtractorType",
]
