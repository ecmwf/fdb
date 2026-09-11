# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Shared mock objects and factory helpers for z3fdb_store tests.

Imported by conftest.py (for fixtures) and individual test files (for
configs and the mock class used in chunk-size assertions).
"""

import math
import struct
from typing_extensions import override

from pychunked_data_view.chunked_data_view import ChunkedDataView
from z3fdb._internal.zarr import FdbSource, FdbZarrArray, FdbZarrGroup, FdbZarrStore


class MockChunkedDataView(ChunkedDataView):
    """Duck-typed stand-in for ChunkedDataView — no real FDB or GRIB needed.

    ``at(index)`` fills each chunk with float32 values equal to the flat chunk
    index, so chunk content is verifiable without real data.
    """

    def __init__(self, shape, chunk_shape, chunks_per_axis, fill_value=0.0):
        assert len(shape) == len(chunk_shape) == len(chunks_per_axis)
        self._shape = shape
        self._chunk_shape = chunk_shape
        self._chunks_per_axis = chunks_per_axis
        self._fill_value = fill_value

    @override
    def shape(self):
        return self._shape

    @override
    def chunk_shape(self):
        return self._chunk_shape

    @override
    def chunks(self):
        return self._chunks_per_axis

    @override
    def fill_missing_value(self):
        return self._fill_value

    @override
    def at(self, index):
        flat = int(sum(i * math.prod(self._chunks_per_axis[j + 1 :]) for j, i in enumerate(index)))
        n = math.prod(self._chunk_shape)
        return struct.pack(f"<{n}f", *[float(flat)] * n)

    def chunk_bytes(self):
        return math.prod(self._chunk_shape) * 4  # float32


# Default 1-D config for tests that only inspect metadata structure
SIMPLE = ((4,), (1,), (4,))

# Per-array configs for the shared deep hierarchy: (shape, chunk_shape, chunks_per_axis)
ARR_ROOT = ((12,), (3,), (4,))  # 1-D  4 chunks  12 B each
ARR_A1 = ((6, 4), (2, 2), (3, 2))  # 2-D  6 chunks  16 B each
ARR_AI1 = ((4, 3, 2), (2, 3, 1), (2, 1, 2))  # 3-D  4 chunks  24 B each
ARR_AI2 = ((8,), (4,), (2,))  # 1-D  2 chunks  16 B each
ARR_BBD = ((4, 2), (2, 1), (2, 2))  # 2-D  4 chunks   8 B each


def make_array(name, cfg=SIMPLE):
    """Create an FdbZarrArray backed by the mock — no real FDB data needed."""
    shape, chunk_shape, chunks_per_axis = cfg
    return FdbZarrArray(
        name=name,
        datasource=FdbSource(MockChunkedDataView(shape, chunk_shape, chunks_per_axis)),
    )


def build_deep_store():
    """Build the shared 4-level group/array hierarchy used across multiple tests.

    root (group)                              — 31 store paths total
    ├── arr_root  1-D  (12,)/(3,)   → 4 chunks
    ├── grp_a
    │   ├── arr_a1   2-D  (6,4)/(2,2)    → 6 chunks
    │   └── grp_a_inner
    │       ├── arr_ai1  3-D  (4,3,2)/(2,3,1)  → 4 chunks
    │       └── arr_ai2  1-D  (8,)/(4,)         → 2 chunks
    └── grp_b  (groups only, no direct arrays)
        └── grp_bb
            └── grp_bbd
                └── arr_bbd  2-D  (4,2)/(2,1)  → 4 chunks
    """
    grp_a_inner = FdbZarrGroup(
        name="grp_a_inner",
        children=[
            make_array("arr_ai1", ARR_AI1),
            make_array("arr_ai2", ARR_AI2),
        ],
    )
    grp_a = FdbZarrGroup(
        name="grp_a",
        children=[
            make_array("arr_a1", ARR_A1),
            grp_a_inner,
        ],
    )
    grp_bbd = FdbZarrGroup(name="grp_bbd", children=[make_array("arr_bbd", ARR_BBD)])
    grp_bb = FdbZarrGroup(name="grp_bb", children=[grp_bbd])
    grp_b = FdbZarrGroup(name="grp_b", children=[grp_bb])
    return FdbZarrStore(FdbZarrGroup(name="", children=[make_array("arr_root", ARR_ROOT), grp_a, grp_b]))
