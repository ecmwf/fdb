# SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0
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
    obj.chunking = Chunking.WHOLE_AXIS
    assert obj.chunking == Chunking.WHOLE_AXIS


def test_axis_definition_individual_chunk():
    obj = AxisDefinition(["key1", "key0"], Chunking.FixedSizeChunk(2))
    assert obj.keys == ["key1", "key0"]
    assert obj.chunking == Chunking.FixedSizeChunk(2)
    obj.keys = []
    assert obj.keys == []
    obj.chunking = Chunking.FixedSizeChunk(2)
    assert obj.chunking == Chunking.FixedSizeChunk(2)


def test_builder(read_only_fdb_setup):
    builder = ChunkedDataViewBuilder(read_only_fdb_setup)
    builder.add_part(
        {
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
        },
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.fill_missing_value(-20.0)
    view = builder.build()

    expected = list(range(0, 5248))

    for a, b in itertools.product(range(0, 32), range(0, 3)):
        np.testing.assert_array_almost_equal_nulp(view.at((a, b, 0)), expected)

    assert view.fill_missing_value() == -20.0
