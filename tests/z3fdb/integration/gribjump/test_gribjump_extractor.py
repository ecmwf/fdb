# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for the GribJump extractor via SimpleStoreBuilder."""

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

_TOTAL_VALUES = 5248

_COMMON = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "date": "2020-01-01/to/2020-01-04",
    "levtype": "sfc",
    "step": 0,
    "param": [167, 131, 132],
    "time": "0/to/21/by/3",
}


@pytest.mark.parametrize(
    ("chunk_size", "n_implicit_chunks"),
    [
        (1312, 4),  # 5248 / 1312 = 4  (exact)
        (2624, 2),  # 5248 / 2624 = 2  (exact)
    ],
    ids=["chunk-1312", "chunk-2624"],
)
def test_gribjump_field_chunking_shape(read_only_fdb_ramp_setup, chunk_size, n_implicit_chunks) -> None:
    """FixedSizeChunking on the implicit axis produces the expected zarr chunk shape.

    The implicit (grid-point) dimension must be split into chunks of *chunk_size*,
    leaving the overall shape unchanged and reflecting the chunk size in the zarr
    metadata.
    """
    builder = SimpleStoreBuilder(read_only_fdb_ramp_setup)
    builder.add_part(
        _COMMON,
        [
            AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunk_shape=8)),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GribJump(field_chunking=Chunking.FixedSizeChunk(chunk_shape=chunk_size)),
    )
    store = builder.build()
    data = zarr.open_array(store)

    assert data.shape == (32, 3, _TOTAL_VALUES), f"unexpected shape {data.shape}"
    assert data.chunks == (8, 1, chunk_size), f"unexpected chunk shape {data.chunks}"
    assert data.nchunks == 4 * 3 * n_implicit_chunks, f"unexpected chunk count {data.nchunks}"


@pytest.mark.parametrize(
    "chunk_size",
    [1312, 2624],
    ids=["chunk-1312", "chunk-2624"],
)
def test_gribjump_field_chunking_values(read_only_fdb_ramp_setup, ramp_expected, chunk_size) -> None:
    """Every field lands in the right slot, with the right grid-point range.

    Uses the ramp fixture, where ``value(field, i) = field * 10000 + i``. That matters: with
    the older fixture every field carried an identical ramp, so this test passed even if the
    key -> buffer-slot mapping was completely scrambled. The per-field term now pins the slot
    and the ``+ i`` term pins the sub-range, so both failure modes are visible.

    Compared with atol=0.5 to absorb GRIB packing, which is far below the smallest difference
    that means anything here: 1 for a neighbouring grid point, 10000 for a wrong field.
    """
    builder = SimpleStoreBuilder(read_only_fdb_ramp_setup)
    builder.add_part(
        _COMMON,
        [
            AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunk_shape=8)),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GribJump(field_chunking=Chunking.FixedSizeChunk(chunk_shape=chunk_size)),
    )
    store = builder.build()
    data = zarr.open_array(store)

    count_params = data.shape[1]
    n_chunks = _TOTAL_VALUES // chunk_size

    # Full assembled slice per (datetime, param): catches a field in the wrong slot.
    for dt_idx in range(data.shape[0]):
        for param_idx in range(count_params):
            np.testing.assert_allclose(
                data[dt_idx, param_idx],
                ramp_expected(dt_idx, param_idx, count_params, _TOTAL_VALUES),
                atol=0.5,
                err_msg=f"full-field mismatch at datetime={dt_idx}, param={param_idx}",
            )

    # Each implicit chunk read on its own: catches a wrong grid-point range.
    expected_last = ramp_expected(data.shape[0] - 1, count_params - 1, count_params, _TOTAL_VALUES)
    for chunk_k in range(n_chunks):
        start = chunk_k * chunk_size
        end = start + chunk_size
        logging.debug(f"Chunk {chunk_k}: {data[-1, -1, start:end]}")
        np.testing.assert_allclose(
            data[-1, -1, start:end],
            expected_last[start:end],
            atol=0.5,
            err_msg=f"sub-range mismatch for implicit chunk {chunk_k} [{start}:{end}]",
        )


@pytest.mark.parametrize(
    "chunk_size",
    [
        1000,  # 5248 % 1000 != 0
        5247,  # one short of the whole field
    ],
    ids=["not-a-divisor", "off-by-one"],
)
def test_gribjump_field_chunk_size_must_divide_the_grid(read_only_fdb_ramp_setup, chunk_size) -> None:
    """A field chunk size that cannot tile the grid must fail at build() and say why.

    The implicit axis is the one dimension a view cannot leave ragged, so the size has to
    divide the grid exactly.
    """
    builder = SimpleStoreBuilder(read_only_fdb_ramp_setup)
    builder.add_part(
        _COMMON,
        [
            AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunk_shape=8)),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GribJump(field_chunking=Chunking.FixedSizeChunk(chunk_shape=chunk_size)),
    )
    with pytest.raises(RuntimeError, match="does not evenly divide"):
        builder.build()



@pytest.mark.parametrize("chunk_size", [None, 1312], ids=["whole-axis", "chunk-1312"])
def test_gribjump_bitmap_missing_values(
    read_only_fdb_bitmap_setup, ramp_expected, bitmap_missing_indices, chunk_size
) -> None:
    """Bitmap-masked grid points read back as the fill value, and the rest are not shifted.

    This is the only coverage of GribJumpExtractor's ``mask[j / 64][j % 64]`` indexing. The
    masked indices deliberately straddle 64-bit word boundaries and the 1312 field-chunk
    boundary, so a word/bit mix-up shows up as a shifted mask rather than passing by luck.
    Parametrised over the chunked case too, since that is where the range offset and the mask
    offset could disagree.
    """
    fill = -1234.0
    field_chunking = None if chunk_size is None else Chunking.FixedSizeChunk(chunk_shape=chunk_size)

    builder = SimpleStoreBuilder(read_only_fdb_bitmap_setup)
    builder.add_part(
        _COMMON,
        [
            AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunk_shape=8)),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GribJump(field_chunking=field_chunking),
    )
    builder.fill_missing_value(fill)
    data = zarr.open_array(builder.build())

    missing = np.array(bitmap_missing_indices)
    present = np.setdiff1d(np.arange(_TOTAL_VALUES), missing)

    for dt_idx, param_idx in ((0, 0), (31, 2)):
        slice_ = data[dt_idx, param_idx]
        expected = ramp_expected(dt_idx, param_idx, data.shape[1], _TOTAL_VALUES)

        np.testing.assert_array_equal(
            slice_[missing],
            np.full(missing.size, fill, dtype=np.float32),
            err_msg=f"masked points not filled at datetime={dt_idx}, param={param_idx}",
        )
        np.testing.assert_allclose(
            slice_[present],
            expected[present],
            atol=0.5,
            err_msg=f"unmasked points shifted at datetime={dt_idx}, param={param_idx}",
        )
