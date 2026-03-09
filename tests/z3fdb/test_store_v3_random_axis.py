import logging

import zarr

from z3fdb import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    SimpleStoreBuilder,
)

logging.basicConfig(level=logging.DEBUG)


def test_random_axis_retrieval(read_only_fdb_pattern_setup) -> None:
    """Test the retrieval of random axis order in a MARS request

    If we permute the order of the request and the values for each individual key,
    we expect this to be part of the resulting data view, as well.

    This means that the permutation needs to be tested and checked against. The values
    for the 'fields' in the data view are injected by the fixture in conftest.py
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    # The individual values are scrambled to check for persistent retrieval on the FDB side
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-03/2020-01-01/2020-01-02,"
        "levtype=sfc,"
        "step=0,"
        "param=167/165/166,"
        "time=18/0/12/6",
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data

    # This is coming from the read_only_fdb_pattern_setup fixtures, the data is filled in there
    # in this pattern

    def value_at(param, time, date):
        date_perm = [2, 0, 1]
        param_perm = [2, 0, 1]
        time_perm = [3, 0, 2, 1]
        return (
            param_perm[param] * 1
            + time_perm[time] * 3 * 1
            + date_perm[date] * 4 * 3 * 1
        )

    # This is the first entire in the fixture which creates constant fields
    # therefore the field has to be a constant 0
    assert value_at(1, 1, 1) == 0

    # Same but for the last entry, therefore the field has to be
    # a constant 35 = 3 dates a 4 time points a 3 params a 1 step - 1 (because 0 indexed)
    assert value_at(0, 0, 0) == 35

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                assert all(data[date, time, param, 0] == value_at(param, time, date))


def test_random_axis_retrieval_swapped_axis_and_request(
    read_only_fdb_pattern_setup,
) -> None:
    """Test the retrieval of random axis order in a MARS request

    If we permute the order of the request and the values for each individual key,
    we expect this to be part of the resulting data view, as well. In this case the AxisDefinitions
    are permuted, as well and we check for the correct behavior

    This means that the permutation needs to be tested and checked against. The values
    for the 'fields' in the data view are injected by the fixture in conftest.py
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    # The individual values are scrambled to check for persistent retrieval on the FDB side
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "time=18/0/12/6,"
        "date=2020-01-03/2020-01-01/2020-01-02,"
        "levtype=sfc,"
        "step=0,"
        "param=167/165/166",
        [
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data

    # This is coming from the read_only_fdb_pattern_setup fixtures, the data is filled in there
    # in this pattern

    def value_at(param, time, date):
        date_perm = [2, 0, 1]  # Due to switching order of date in the request
        param_perm = [2, 0, 1]
        time_perm = [3, 0, 2, 1]
        return (
            param_perm[param] * 1
            + time_perm[time] * 3 * 1
            + date_perm[date] * 4 * 3 * 1
        )

    # This is the first entire in the fixture which creates constant fields
    # therefore the field has to be a constant 0
    assert value_at(1, 1, 1) == 0

    # Same but for the last entry, therefore the field has to be
    # a constant 35 = 3 dates a 4 time points a 3 params a 1 step - 1 (because 0 indexed)
    assert value_at(0, 0, 0) == 35

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                assert all(data[time, 0, param, date] == value_at(param, time, date))


def test_random_axis_retrieval_swapped_axis_and_request_combined_axis(
    read_only_fdb_pattern_setup,
) -> None:
    """Test the retrieval of random axis order in a MARS request

    If we permute the order of the request and the values for each individual key,
    we expect this to be part of the resulting data view, as well. In this case the AxisDefinitions
    are permuted and combined, as well. Additionally the chunking is disabled which leads to the whole
    request being fetch at once.

    This means that the permutation needs to be tested and checked against. The values
    for the 'fields' in the data view are injected by the fixture in conftest.py
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    # The individual values are scrambled to check for persistent retrieval on the FDB side
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "time=18/0/12/6,"
        "date=2020-01-03/2020-01-01/2020-01-02,"
        "levtype=sfc,"
        "step=0,"
        "param=167/165/166",
        [
            AxisDefinition(["time", "step", "param", "date"], Chunking.NONE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data

    # This is coming from the read_only_fdb_pattern_setup fixtures, the data is filled in there
    # in this pattern

    def value_at(param, time, date):
        date_perm = [2, 0, 1]  # Due to switching order of date in the request
        param_perm = [2, 0, 1]
        time_perm = [3, 0, 2, 1]
        return (
            param_perm[param] * 1
            + time_perm[time] * 3 * 1
            + date_perm[date] * 4 * 3 * 1
        )

    def linearize(time, param, date):
        return date + param * 3 + time * 3 * 3

    # This is the first entire in the fixture which creates constant fields
    # therefore the field has to be a constant 0
    assert value_at(1, 1, 1) == 0

    # Same but for the last entry, therefore the field has to be
    # a constant 35 = 3 dates a 4 time points a 3 params a 1 step - 1 (because 0 indexed)
    assert value_at(0, 0, 0) == 35

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                assert all(
                    data[linearize(time, param, date)] == value_at(param, time, date)
                )
