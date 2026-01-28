# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.


import pytest
import zarr
import numpy as np

from z3fdb import (
    AxisDefinition,
    Chunking,
    SimpleStoreBuilder,
    ExtractorType,
)


def test_zarr_use_spec_v2(read_only_fdb_setup) -> None:
    assert zarr.config.get("default_zarr_format") == 3


def test_access(read_only_fdb_setup) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_setup)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "step=0,"
        "param=167/131/132,"
        "time=0/to/21/by/3",
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()
    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data
    print(data[:, :])
    print(data.shape)


def test_axis_check_out_of_bounds(read_only_fdb_setup_for_sfc_pl_example) -> None:
    """This test checks whether an access to an axis which has no pendant in the data is failing.
    The request below has param 167 which is not given in the data of the setup fdb. Therefore this
    needs to fail. Accessing the first two params is fine.
    """
    builder = SimpleStoreBuilder(read_only_fdb_setup_for_sfc_pl_example)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/2020-01-02,"
        "levtype=sfc,"
        "step=0,"
        "param=165/166/167,"
        "time=0/to/21/by/3",
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
