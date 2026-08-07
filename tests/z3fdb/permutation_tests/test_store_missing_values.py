# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""Online z3fdb tests.

These tests download real ECMWF open data to verify that bitmap-masked grid points
(e.g. land points in a significant-wave-height field) are returned as the configured
fill value rather than the raw eccodes missingValue sentinel.

Requires:
  pip install ecmwf-opendata
"""

import datetime
import logging

import eccodes as ec
import numpy as np
import pytest
import zarr

from pychunked_data_view import (
    AxisDefinition,
    ChunkedDataViewBuilder,
    Chunking,
    ExtractorType,
)
from pyfdb import FDB
from z3fdb import SimpleStoreBuilder

pytestmark = pytest.mark.online


@pytest.fixture(scope="module")
def swh_grib_download(tmp_path_factory):
    """Download a real SWH GRIB field from ECMWF open data (runs once per module).

    Returns (grib_path, expected_float32_values, missing_mask, date) where:
      - grib_path: path to the downloaded GRIB file
      - expected_float32_values: raw float32 values as eccodes returns them
      - missing_mask: boolean array, True where the GRIB bitmap marks the point missing
      - date: the date string used in the MARS request
    """
    opendata = pytest.importorskip(
        "ecmwf.opendata", reason="pip install ecmwf-opendata to run online tests"
    )

    date = datetime.date.today() - datetime.timedelta(days=2)
    tmp = tmp_path_factory.mktemp("swh_online")
    grib = tmp / "swh_opendata.grib2"

    opendata.Client(source="ecmwf", model="ifs", resol="0p25").retrieve(
        date=date.isoformat(),
        time=0,
        type="fc",
        stream="wave",
        step=0,
        param="swh",
        target=str(grib),
    )

    with open(grib, "rb") as f:
        gid = ec.codes_grib_new_from_file(f)
    expected = ec.codes_get_values(gid).astype(np.float32)
    missing_sentinel = float(ec.codes_get(gid, "missingValue"))
    missing = expected == np.float32(missing_sentinel)
    ec.codes_release(gid)

    logging.debug(f"#Values: {len(expected)}")
    logging.debug(f"#Missing: {np.sum(missing)}")
    logging.debug(f"#Missing Entries/#Entries: {np.sum(missing)} / {len(expected)}")

    assert 0 < missing.sum() < len(expected), (
        "SWH field must have both missing and non-missing values for this test to be meaningful"
    )

    return grib, expected, missing, date


@pytest.fixture
def fdb_with_swh_bitmap(empty_fdb, swh_grib_download):
    """Archive the downloaded SWH field into a fresh empty FDB.

    Returns (config_path, expected_float32_values, missing_mask, date).
    Uses the empty_fdb fixture so the FDB infrastructure is not duplicated here.
    """
    config_path = empty_fdb
    grib, expected, missing, date = swh_grib_download

    fdb = FDB(config_path.read_text())
    fdb.archive(grib.read_bytes())
    fdb.flush()

    return config_path, expected, missing, date


def test_bitmap_missing_points_become_fill_value(fdb_with_swh_bitmap):
    """Bitmap-masked grid points must be returned as fill_value, not the eccodes sentinel."""
    config_path, expected, missing, date = fdb_with_swh_bitmap

    builder = ChunkedDataViewBuilder(config_path)
    builder.add_part(
        {
            "class": "od",
            "expver": "0001",
            "stream": "wave",
            "domain": "g",
            "type": "fc",
            "levtype": "sfc",
            "date": date.strftime("%Y%m%d"),
            "time": "0000",
            "step": 0,
            "param": 140229,
        },
        [AxisDefinition(["step"], Chunking.SINGLE_VALUE)],
        ExtractorType.GRIB,
    )
    builder.fill_missing_value(-20.0)
    view = builder.build()

    assert view.fill_missing_value() == -20.0

    values = view.at((0, 0))
    np.testing.assert_array_equal(values[missing], view.fill_missing_value())
    np.testing.assert_array_equal(values[~missing], expected[~missing])


def test_fill_value_propagated_to_zarr_metadata(fdb_with_swh_bitmap):
    """fill_value set on the builder must appear in the zarr array metadata."""
    config_path, _, _, date = fdb_with_swh_bitmap

    builder = SimpleStoreBuilder(config_path)
    builder.add_part(
        {
            "class": "od",
            "expver": "0001",
            "stream": "wave",
            "domain": "g",
            "type": "fc",
            "levtype": "sfc",
            "date": date.strftime("%Y%m%d"),
            "time": "0000",
            "step": 0,
            "param": 140229,
        },
        [AxisDefinition(["step"], Chunking.SINGLE_VALUE)],
        ExtractorType.GRIB,
    )
    builder.fill_missing_value(-20.0)
    store = builder.build()

    arr = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert arr.fill_value == -20.0


def test_default_fill_value_replaces_bitmap_sentinel(fdb_with_swh_bitmap):
    """Without an explicit fill_value call, the default (NaN) replaces the eccodes sentinel."""
    config_path, expected, missing, date = fdb_with_swh_bitmap

    builder = ChunkedDataViewBuilder(config_path)
    builder.add_part(
        {
            "class": "od",
            "expver": "0001",
            "stream": "wave",
            "domain": "g",
            "type": "fc",
            "levtype": "sfc",
            "date": date.strftime("%Y%m%d"),
            "time": "0000",
            "step": 0,
            "param": 140229,
        },
        [AxisDefinition(["step"], Chunking.SINGLE_VALUE)],
        ExtractorType.GRIB,
    )
    view = builder.build()

    values = view.at((0, 0))
    assert np.all(np.isnan(values[missing])), "missing points should be NaN by default"
    np.testing.assert_array_equal(values[~missing], expected[~missing])
