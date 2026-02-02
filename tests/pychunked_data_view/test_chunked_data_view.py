# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import numpy as np
import itertools


from pychunked_data_view import (
    AxisDefinition,
    ChunkedDataViewBuilder,
    Chunking,
    ExtractorType,
)


def test_axis_definition_can_construct():
    obj = AxisDefinition(["key1", "key0"], Chunking.SINGLE_VALUE)
    assert obj.keys == ["key1", "key0"]
    assert obj.chunking == Chunking.SINGLE_VALUE


def test_axis_definition_can_assign():
    obj = AxisDefinition(["key1", "key0"], Chunking.SINGLE_VALUE)
    assert obj.keys == ["key1", "key0"]
    assert obj.chunking == Chunking.SINGLE_VALUE
    obj.keys = []
    assert obj.keys == []
    obj.chunking = Chunking.SINGLE_VALUE
    assert obj.chunking == Chunking.SINGLE_VALUE


def test_builder(read_only_fdb_setup):
    builder = ChunkedDataViewBuilder(read_only_fdb_setup)
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
    view = builder.build()

    expected = list(range(0, 5248))
    for a, b in itertools.product(range(0, 32), range(0, 2)):
        np.testing.assert_array_almost_equal_nulp(view.at((a, b)), expected)
