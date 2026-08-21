# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0
"""Integration test: SimpleStoreBuilder with one GRIB part and one GribJump part.

Data layout (from ``build_grib_messages`` in conftest.py):
  - 4 dates (2020-01-01 .. 2020-01-04) x 8 times (0,3,..,21 h) = 32 datetime entries
  - 3 params: 167, 131, 132  (all fields carry values list(range(0, grid_points)))

The test splits params across two extractors:
  - Part 1 (Grib):     params [167, 131]
  - Part 2 (GribJump): param  [132]

The datetime axis uses FixedSizeChunking(8) -> 4 chunks of 8.
The param axis uses SingleValueChunking and is the extension axis.

Resulting view shape: (32, 3, grid_points)
Chunk shape:          (8,  1, grid_points)
"""

import logging

import numpy as np
import pytest
import zarr

from z3fdb import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    SimpleStoreBuilder,
)

log = logging.getLogger(__name__)

pytestmark = pytest.mark.offline

_COMMON = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "date": "2020-01-01/to/2020-01-04",
    "levtype": "sfc",
    "step": 0,
    "time": "0/to/21/by/3",
}

# 4 dates x 8 times = 32 entries -> 4 chunks of 8
_DATETIME_AXIS = AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunk_shape=8))
_PARAM_AXIS = AxisDefinition(["param"], Chunking.SINGLE_VALUE)


def test_grib_and_gribjump_parts(read_only_fdb_setup) -> None:
    """A view built from a GRIB part and a GribJump part yields identical data.

    Both extractors read the same underlying FDB. The GRIB extractor performs a
    full-field decode; the GribJump extractor reads the same values without
    decoding the full GRIB message. Since the fixture writes identical values
    (range(0, grid_points)) into every field, both parts should return the same
    numpy array contents regardless of which extractor served them.
    """
    builder = SimpleStoreBuilder(read_only_fdb_setup)

    # Part 1 - standard GRIB decode (params 167 and 131)
    builder.add_part(
        {**_COMMON, "param": [167, 131]},
        [_DATETIME_AXIS, _PARAM_AXIS],
        ExtractorType.Grib(),
    )

    # Part 2 - GribJump partial decode (param 132)
    builder.add_part(
        {**_COMMON, "param": [132]},
        [_DATETIME_AXIS, _PARAM_AXIS],
        ExtractorType.GribJump(),
    )

    builder.extend_on_axis(1)
    store = builder.build()

    data = zarr.open_array(store)
    log.debug("shape=%s  chunks=%s", data.shape, data.chunks)

    assert data.shape[:2] == (32, 3), f"expected (32, 3, ...), got {data.shape}"
    assert data.chunks[:2] == (8, 1), f"expected chunks (8, 1, ...), got {data.chunks}"

    # The fixture writes range(0, grid_points) into every field, so every
    # (datetime_index, param_index) slice should equal that ramp regardless
    # of which extractor served it.
    grid_points = data.shape[-1]
    expected = np.arange(grid_points, dtype=np.float32)

    for dt_idx in range(data.shape[0]):
        for param_idx in range(data.shape[1]):
            np.testing.assert_array_equal(
                data[dt_idx, param_idx],
                expected,
                err_msg=f"mismatch at datetime={dt_idx}, param={param_idx}",
            )
