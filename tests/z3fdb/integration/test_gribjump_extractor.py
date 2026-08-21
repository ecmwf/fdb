# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for the GribJump extractor via SimpleStoreBuilder."""

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
def test_gribjump_field_chunking_shape(read_only_fdb_setup, chunk_size, n_implicit_chunks) -> None:
    """FixedSizeChunking on the implicit axis produces the expected zarr chunk shape.

    The implicit (grid-point) dimension must be split into chunks of *chunk_size*,
    leaving the overall shape unchanged and reflecting the chunk size in the zarr
    metadata.
    """
    builder = SimpleStoreBuilder(read_only_fdb_setup)
    builder.add_part(
        _COMMON,
        [
            AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunk_shape=8)),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GribJump(chunking=Chunking.FixedSizeChunk(chunk_shape=chunk_size)),
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
def test_gribjump_field_chunking_values(read_only_fdb_setup, chunk_size) -> None:
    """Values across implicit-axis chunk boundaries are correctly assembled.

    Each field carries values ``list(range(0, 5248))``.  With FixedSizeChunking on
    the implicit axis, zarr fetches each chunk independently; the reconstructed array
    must equal ``np.arange(5248)`` for every (datetime, param) combination, confirming
    that adjacent chunks are stitched back together correctly.
    """
    builder = SimpleStoreBuilder(read_only_fdb_setup)
    builder.add_part(
        _COMMON,
        [
            AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunk_shape=8)),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GribJump(chunking=Chunking.FixedSizeChunk(chunk_shape=chunk_size)),
    )
    store = builder.build()
    data = zarr.open_array(store)

    expected_full = np.arange(_TOTAL_VALUES, dtype=np.float32)
    n_chunks = _TOTAL_VALUES // chunk_size

    # Verify the full assembled slice for a sample of (datetime, param) pairs.
    for dt_idx in range(data.shape[0]):
        for param_idx in range(data.shape[1]):
            np.testing.assert_array_equal(
                data[dt_idx, param_idx],
                expected_full,
                err_msg=f"full-field mismatch at datetime={dt_idx}, param={param_idx}",
            )

    # Verify each individual implicit chunk directly using sub-indexing, confirming
    # that values within each chunk are the correct slice of the field sequence.
    for chunk_k in range(n_chunks):
        start = chunk_k * chunk_size
        end = start + chunk_size
        logging.debug(f"Chunk {chunk_k}: {data[0, 0, start:end]}")
        np.testing.assert_array_equal(
            data[0, 0, start:end],
            expected_full[start:end],
            err_msg=f"sub-range mismatch for implicit chunk {chunk_k} [{start}:{end}]",
        )
