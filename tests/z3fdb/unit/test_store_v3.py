# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0


import logging

import numpy as np
import pytest
import zarr
from zarr.core.buffer import default_buffer_prototype
from zarr.core.sync import _collect_aiterator, sync

from z3fdb import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    SimpleStoreBuilder,
)

log = logging.getLogger(__name__)

pytestmark = pytest.mark.offline

_MARS_REQUEST = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "date": "2020-01-01/to/2020-01-04",
    "levtype": "sfc",
    "step": "0",
    "param": "167/131/132",
    "time": "0/to/21/by/3",
}

_AXES = [
    AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
    AxisDefinition(["param"], Chunking.SINGLE_VALUE),
]


def test_zarr_use_spec_v2(read_only_fdb_setup) -> None:
    assert zarr.config.get("default_zarr_format") == 3


def test_access(read_only_fdb_setup) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_setup)
    builder.add_part(_MARS_REQUEST, _AXES, ExtractorType.GRIB)
    store = builder.build()
    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data
    log.debug("shape=%s", data.shape)
    log.debug("data[:, :]=%s", data[:, :])


def test_axis_check_out_of_bounds(read_only_fdb_setup_for_sfc_pl_example) -> None:
    """This test checks whether an access to an axis which has no pendant in the data is failing.
    The request below has param 167 which is not given in the data of the setup fdb. Therefore this
    needs to fail. Accessing the first two params is fine.
    """
    builder = SimpleStoreBuilder(read_only_fdb_setup_for_sfc_pl_example)
    builder.add_part(
        {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": ["2020-01-01", "2020-01-02"],
            "levtype": "sfc",
            "step": 0,
            "param": [165, 166, 167],
            "time": "0/to/21/by/3",
        },
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()
    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data

    for i in range(16):
        assert np.all(data[i, 0] == 2 * i)
        assert np.all(data[i, 1] == (2 * i) + 1)

    with pytest.raises(Exception):
        data[0, 2]


def test_store_list_chunks_complete(read_only_fdb_setup) -> None:
    """Every chunk key from store.list() can be retrieved and has the correct data.

    Setup (build_grib_messages in conftest.py):
      - 4 dates (2020-01-01 .. 2020-01-04) × 8 times (0,3,..,21h) = 32 datetime chunks
      - 3 params (167, 131, 132) = 3 param chunks
      - 96 total GRIB fields → 96 chunk keys + 1 zarr.json = 97 store keys
      - All fields carry identical values: range(0, grid_points) as float32
    """
    builder = SimpleStoreBuilder(read_only_fdb_setup)
    builder.add_part(_MARS_REQUEST, _AXES, ExtractorType.GRIB)
    store = builder.build()

    # Expected key counts derived from the fixture's data generation
    EXPECTED_DATES = 4
    EXPECTED_TIMES = 8  # 0, 3, 6, 9, 12, 15, 18, 21
    EXPECTED_PARAMS = 3  # 167, 131, 132
    EXPECTED_CHUNKS = EXPECTED_DATES * EXPECTED_TIMES * EXPECTED_PARAMS  # 96

    all_keys = sync(_collect_aiterator(store.list()))
    chunk_keys = [k for k in all_keys if k.startswith("c/")]
    log.debug(
        "store.list(): %d total keys (%d zarr.json, %d chunk keys)",
        len(all_keys),
        len(all_keys) - len(chunk_keys),
        len(chunk_keys),
    )

    assert len(all_keys) == EXPECTED_CHUNKS + 1, f"expected {EXPECTED_CHUNKS + 1} total keys, got {len(all_keys)}"
    assert len(chunk_keys) == EXPECTED_CHUNKS, f"expected {EXPECTED_CHUNKS} chunk keys, got {len(chunk_keys)}"

    # Determine grid size from the zarr metadata so we don't hard-code it
    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    grid_points = data.shape[-1]
    expected_bytes = grid_points * 4  # float32 = 4 bytes per value
    log.debug(
        "array shape=%s, grid_points=%d, expected_bytes_per_chunk=%d",
        data.shape,
        grid_points,
        expected_bytes,
    )

    # Every chunk must be retrievable, the right size, and carry the correct values.
    # build_grib_messages fills every GRIB field with list(range(0, grid_points)).
    expected_values = np.arange(grid_points, dtype=np.float32)

    for key in chunk_keys:
        buf = sync(store.get(key, prototype=default_buffer_prototype()))
        assert buf is not None, f"chunk {key!r} returned None"
        raw = buf.to_bytes()
        assert len(raw) == expected_bytes, f"chunk {key!r}: expected {expected_bytes} bytes, got {len(raw)}"
        actual_values = np.frombuffer(raw, dtype=np.float32)
        log.debug("chunk %r: %d bytes, values[0:3]=%s", key, len(raw), actual_values[:3])
        np.testing.assert_array_equal(
            actual_values,
            expected_values,
            err_msg=f"chunk {key!r} has unexpected values",
        )
