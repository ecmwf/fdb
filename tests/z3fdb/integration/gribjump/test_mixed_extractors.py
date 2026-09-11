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

from chunked_data_view_bindings import has_gribjump_extractor
from z3fdb import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    SimpleStoreBuilder,
)

log = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.offline,
    pytest.mark.gribjump,
    pytest.mark.skipif(
        not has_gribjump_extractor,
        reason=(
            "build has no GribJump extractor; configure with "
            "-DENABLE_ZARR_GRIBJUMP_EXTRACTOR=ON (requires a bundle build providing gribjump)"
        ),
    ),
]

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


def test_grib_and_gribjump_parts(read_only_fdb_ramp_setup, ramp_expected) -> None:
    """A GRIB part and a GribJump part agree on both values and field ordering.

    Both extractors read the same FDB; GRIB does a full decode, GribJump jumps to the values.
    The ramp fixture gives every field a distinct base, so this checks the two extractors put
    the *same field* in the *same slot* — with the older fixture every field carried an
    identical ramp, so a scrambled key -> slot mapping would still have passed.

    Compared with atol=0.5 to absorb GRIB packing, well under the 1 / 10000 differences that
    a wrong grid point or a wrong field would produce.
    """
    builder = SimpleStoreBuilder(read_only_fdb_ramp_setup)

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

    # param axis order is [167, 131] from the GRIB part then [132] from the GribJump part,
    # which is the order the ramp fixture wrote them in, so the expectation is the plain
    # field index. Slots 0 and 1 are served by GribExtractor, slot 2 by GribJumpExtractor.
    grid_points = data.shape[-1]
    count_params = data.shape[1]

    for dt_idx in range(data.shape[0]):
        for param_idx in range(count_params):
            np.testing.assert_allclose(
                data[dt_idx, param_idx],
                ramp_expected(dt_idx, param_idx, count_params, grid_points),
                atol=0.5,
                err_msg=f"mismatch at datetime={dt_idx}, param={param_idx}",
            )


@pytest.mark.parametrize("gribjump_first", [False, True], ids=["grib-first", "gribjump-first"])
def test_field_chunking_mixed_with_grib_is_rejected(read_only_fdb_ramp_setup, gribjump_first) -> None:
    """A GribJump part using field chunking cannot be mixed with a GRIB part.

    ``GribExtractor`` always returns the whole field, so its chunk on the implicit axis is the
    full grid; a ``GribJumpExtractor`` with ``FixedSizeChunk`` writes a smaller block. The view
    takes that extent from the *first* part only, so the mismatch must be rejected at build()
    time — otherwise the part whose block is larger overruns its slots at read time while still
    reporting the expected message count.
    """
    grib_part = ({**_COMMON, "param": [167, 131]}, ExtractorType.Grib())
    gribjump_part = (
        {**_COMMON, "param": [132]},
        ExtractorType.GribJump(field_chunking=Chunking.FixedSizeChunk(chunk_shape=1312)),
    )

    first, second = (gribjump_part, grib_part) if gribjump_first else (grib_part, gribjump_part)

    builder = SimpleStoreBuilder(read_only_fdb_ramp_setup)
    builder.add_part(first[0], [_DATETIME_AXIS, _PARAM_AXIS], first[1])
    builder.add_part(second[0], [_DATETIME_AXIS, _PARAM_AXIS], second[1])
    builder.extend_on_axis(1)

    with pytest.raises(RuntimeError, match="WholeAxisChunking") as exc:
        builder.build()

    logging.debug(f"{exc.value.args[0]}")
