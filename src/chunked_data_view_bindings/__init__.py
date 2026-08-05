# libfdb5.so and ependencies have to be loaded prior to importing
# pychunked_data_view
import findlibs

findlibs.load("fdb5")

from chunked_data_view_bindings.chunked_data_view_bindings import (
    init_bindings,
    AxisDefinition,
    AxisDefinition.WholeAxisChunking,
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
    "AxisDefinition.WholeAxisChunking",
    "ExtractorType",
]
