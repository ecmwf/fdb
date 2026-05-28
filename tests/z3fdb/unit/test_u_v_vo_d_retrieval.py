# (C) Copyright 2025- ECMWF.

# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import numpy as np
import pytest
import zarr

from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

pytestmark = pytest.mark.offline

# Fixture data layout (read_only_fdb_setup_for_div_vo_example):
#   itertools.product(dates, times, levels, parameters)
#   dates      = [20200101, 20200102]          → 2 values
#   times      = [0, 300, ..., 2100]           → 8 values
#   levels     = [50, 100]                     → 2 values
#   parameters = [155, 138]  (d=0, vo=1)       → 2 values
#
# stored value = date_idx*32 + time_idx*4 + level_idx*2 + param_idx

_BASE_REQUEST = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "date": ["2020-01-01", "2020-01-02"],
    "levtype": "pl",
    "step": 0,
    "levelist": [50, 100],
    "time": "0/to/21/by/3",
}


def test_retrieve_u_and_v(read_only_fdb_setup_for_div_vo_example) -> None:
    """
    Retrieve both u (131) and v (132) and verify the data cube values.
    FDB retrieves vo and div, if u and v aren't available.

    Axis layout:
      dim 0 — date x time  (16 entries, date varies slowest)
      dim 1 — param x levelist  (4 entries: (u,50), (u,100), (v,50), (v,100))
      dim 2 — grid points (implicit)
    """
    builder = SimpleStoreBuilder(read_only_fdb_setup_for_div_vo_example)
    builder.add_part(
        # {**_BASE_REQUEST, "param": [155, 138]},
        {**_BASE_REQUEST, "param": [131, 132]},
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )

    with pytest.raises(RuntimeError, match=".*FDB returned paramId=.*"):
        builder.build()


def test_retrieve_vo_and_v(read_only_fdb_setup_for_div_vo_example) -> None:
    builder1 = SimpleStoreBuilder(read_only_fdb_setup_for_div_vo_example)
    builder1.add_part(
        {**_BASE_REQUEST, "param": [138, 132]},
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )

    with pytest.raises(RuntimeError, match=".*FDB returned paramId=.*"):
        builder1.build()


def test_retrieve_vo_and_d(read_only_fdb_setup_for_div_vo_example) -> None:
    """Retrieve vo (138) and d (155) — both stored directly in the FDB.

    Axis layout:
      dim 0 — date × time  (16 entries, date varies slowest)
      dim 1 — param × levelist  (4 entries, param varies slowest)
                index 0: (vo=138, level=50)
                index 1: (vo=138, level=100)
                index 2: (d=155,  level=50)
                index 3: (d=155,  level=100)
      dim 2 — grid points
    """
    builder = SimpleStoreBuilder(read_only_fdb_setup_for_div_vo_example)
    builder.add_part(
        {**_BASE_REQUEST, "param": [138, 155]},
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    data = zarr.open_array(builder.build(), mode="r", zarr_format=3, use_consolidated=False)

    assert data.shape[:2] == (16, 4)

    # value = date_idx*32 + time_idx*4 + level_idx*2 + param_idx
    # param_idx in fixture: 0=d(155), 1=vo(138)
    # level_idx: 0=50, 1=100

    # date=2020-01-01 (idx 0), time=0 (idx 0)
    assert all(data[0, 0] == 1)   # vo, level=50:  0+0+0+1 = 1
    assert all(data[0, 1] == 3)   # vo, level=100: 0+0+2+1 = 3
    assert all(data[0, 2] == 0)   # d,  level=50:  0+0+0+0 = 0
    assert all(data[0, 3] == 2)   # d,  level=100: 0+0+2+0 = 2
    # date=2020-01-01, time=300 (idx 1)
    assert all(data[1, 0] == 5)   # vo, level=50:  0+4+0+1 = 5
    # date=2020-01-02 (idx 1), time=0
    assert all(data[8, 0] == 33)  # vo, level=50:  32+0+0+1 = 33
    assert all(data[8, 2] == 32)  # d,  level=50:  32+0+0+0 = 32
