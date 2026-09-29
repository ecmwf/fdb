# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Exhaustive parametrized tests covering all axis definition permutations.

Two combinatorial sweeps:

1. All 24 permutations of four individually-chunked axes
   (date, time, param, step — each as SINGLE_VALUE).

2. All 6 permutations and 8 chunking mode combinations for three axes where
   date and time are merged into a single axis.
"""

import logging
from itertools import permutations, product

import pytest
import zarr

from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

log = logging.getLogger(__name__)

pytestmark = pytest.mark.offline

CANONICAL_REQUEST = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "levtype": "sfc",
    "step": 0,
    "date": "2020-01-01/to/2020-01-03",
    "param": [165, 166, 167],
    "time": "0/to/21/by/6",
}


def _open_array(store):
    return zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)


def _canonical_value(date, time, param, step=0):
    return step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1


@pytest.mark.parametrize("index_permutation", permutations([0, 1, 2, 3]))
def test_all_four_axis_permutations_chunked(read_only_fdb_pattern_setup, index_permutation) -> None:
    """All 24 permutations of four individually-chunked axes produce correct data."""
    axes = [
        AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        AxisDefinition(["time"], Chunking.SINGLE_VALUE),
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        AxisDefinition(["step"], Chunking.SINGLE_VALUE),
    ]
    axis_names = ["date", "time", "param", "step"]

    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        CANONICAL_REQUEST,
        [axes[i] for i in index_permutation],
        ExtractorType.GRIB,
    )
    data = _open_array(builder.build())
    assert data

    log.debug("permutation: %s", [axis_names[i] for i in index_permutation])
    log.debug("shape: %s", data.shape)

    for date in range(3):
        for time in range(4):
            for param in range(3):
                cur_index = [0, 0, 0, 0]
                cur_index[index_permutation.index(0)] = date
                cur_index[index_permutation.index(1)] = time
                cur_index[index_permutation.index(2)] = param
                cur_index[index_permutation.index(3)] = 0  # step
                assert all(data[*cur_index] == _canonical_value(date, time, param))


_THREE_AXIS_PERMUTATION_CASES = list(
    product(
        permutations([0, 1, 2]),
        product([Chunking.SINGLE_VALUE, Chunking.WHOLE_AXIS], repeat=3),
    )
)


@pytest.mark.parametrize("index_permutation, chunking_modes", _THREE_AXIS_PERMUTATION_CASES)
def test_three_axis_permutations_with_merged_date_time(
    read_only_fdb_pattern_setup, index_permutation, chunking_modes
) -> None:
    """All 6 permutations × 8 chunking combinations for three axes (date+time merged)."""
    axes = [
        AxisDefinition(["date", "time"], chunking_modes[0]),
        AxisDefinition(["param"], chunking_modes[1]),
        AxisDefinition(["step"], chunking_modes[2]),
    ]
    axis_names = ["datetime", "param", "step"]

    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        CANONICAL_REQUEST,
        [axes[i] for i in index_permutation],
        ExtractorType.GRIB,
    )
    data = _open_array(builder.build())
    assert data

    log.debug("permutation: %s", [axis_names[i] for i in index_permutation])
    log.debug("chunking: %s", chunking_modes)
    log.debug("shape: %s, chunks: %s", data.shape, data.chunks)

    for date in range(3):
        for time in range(4):
            for param in range(3):
                datetime = time + date * 4
                cur_index = [0, 0, 0]
                cur_index[index_permutation.index(0)] = datetime
                cur_index[index_permutation.index(1)] = param
                cur_index[index_permutation.index(2)] = 0  # step
                assert all(data[*cur_index] == _canonical_value(date, time, param))
