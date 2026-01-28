import logging
from itertools import permutations, product

import pytest
import zarr

from z3fdb import (
    AxisDefinition,
    Chunking,
    SimpleStoreBuilder,
    ExtractorType,
)

logging.basicConfig(level=logging.DEBUG)


def test_access_pattern(read_only_fdb_pattern_setup) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-03,"
        "levtype=sfc,"
        "step=0,"
        "param=165/166/167,"
        "time=0/to/21/by/6",
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
    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    assert all(data[date, time, param, step] == index)


@pytest.mark.parametrize("index_permutation", permutations([0, 1, 2, 3]))
def test_access_pattern_shuffled_chunked(
    read_only_fdb_pattern_setup, index_permutation
) -> None:
    """Main idea is to test all permutations of the indices in a permuted way and check whether
    the received data aligns with the expected
    """
    axis = [
        AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        AxisDefinition(["time"], Chunking.SINGLE_VALUE),
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        AxisDefinition(["step"], Chunking.SINGLE_VALUE),
    ]
    axis_names = ["date", "time", "param", "step"]
    axis_permutation = [axis[i] for i in index_permutation]

    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-03,"
        "levtype=sfc,"
        "step=0,"
        "param=165/166/167,"
        "time=0/to/21/by/6",
        axis_permutation,
        ExtractorType.GRIB,
    )
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data

    logging.debug(f"Permutation: ({[axis_names[i] for i in index_permutation]})")

    def compute_value(date, time, param, step):
        return step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1

    logging.debug(data.shape)
    logging.debug(data[:, 0, 0, 0])

    # This is coming from the read_only_fdb_pattern_setup fixtures, the data is filled in there
    # in this pattern
    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    cur_index = [0, 0, 0, 0]

                    # Find the index the date gets mapped to, was index 0 in the non-permuted array
                    # Same for all other elements
                    cur_index[index_permutation.index(0)] = date
                    cur_index[index_permutation.index(1)] = time
                    cur_index[index_permutation.index(2)] = param
                    cur_index[index_permutation.index(3)] = step

                    value = compute_value(date=date, time=time, param=param, step=step)

                    logging.debug(f"({date}, {time}, {param}, {step})")
                    logging.debug(f"Permutation: {index_permutation}")
                    logging.debug(f"CurIndex: {cur_index}")
                    logging.debug(f"Value: {value}")
                    logging.debug(f"Data: {data[*cur_index]}")

                    assert all(data[*cur_index] == value)


test_data = product(
    permutations([0, 1, 2]),
    product([Chunking.SINGLE_VALUE, Chunking.NONE], repeat=3),
)


@pytest.mark.parametrize("index_permutation, chunked_permutations", test_data)
def test_access_pattern_shuffled_partially_chunked(
    read_only_fdb_pattern_setup, index_permutation, chunked_permutations
) -> None:
    """Main idea is to test all permutations of the indices in a permuted way and check whether
    the received data aligns with the expected. Same as test above but with additional merged axis and
    permuted chunked stati.
    """
    axis = [
        AxisDefinition(["date", "time"], chunked_permutations[0]),
        AxisDefinition(["param"], chunked_permutations[1]),
        AxisDefinition(["step"], chunked_permutations[2]),
    ]
    axis_names = ["datetime", "param", "step"]
    axis_permutation = [axis[i] for i in index_permutation]

    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-03,"
        "levtype=sfc,"
        "step=0,"
        "param=165/166/167,"
        "time=0/to/21/by/6",
        axis_permutation,
        ExtractorType.GRIB,
    )
    store = builder.build()
    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data

    logging.debug(f"Permutation: ({[axis_names[i] for i in index_permutation]})")
    logging.debug(f"Permutation chunked: ({[i for i in chunked_permutations]})")

    def compute_value(date, time, param, step):
        return step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1

    logging.debug(f"Zarr array shape={data.shape}")
    logging.debug(f"Chunk shape={data.chunks}")
    logging.debug(data[:, 0, 0])

    # This is coming from the read_only_fdb_pattern_setup fixtures, the data is filled in there
    # in this pattern
    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    datetime = date * 4 + time
                    # Find the index the date gets mapped to, was index 0 in the non-permuted array
                    # Same for all other elements
                    cur_index = [0, 0, 0]
                    cur_index[index_permutation.index(0)] = datetime
                    cur_index[index_permutation.index(1)] = param
                    cur_index[index_permutation.index(2)] = step

                    value = compute_value(date=date, time=time, param=param, step=step)

                    logging.debug(f"({date}, {time}, {param}, {step})")
                    logging.debug(f"Permutation: {index_permutation}")
                    logging.debug(f"CurIndex: {cur_index}")
                    logging.debug(f"Value: {value}")
                    logging.debug(f"Data: {data[*cur_index]}")

                    assert all(data[*cur_index] == value)


def test_access_pattern_non_chunked(read_only_fdb_pattern_setup) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-03,"
        "levtype=sfc,"
        "step=0,"
        "param=165/166/167,"
        "time=0/to/21/by/6",
        [
            AxisDefinition(["date"], Chunking.NONE),
            AxisDefinition(["time"], Chunking.NONE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()
    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    assert all(data[date, time, param, step] == index)


def test_access_pattern_non_chunked_mixed(read_only_fdb_pattern_setup) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-03,"
        "levtype=sfc,"
        "step=0,"
        "param=165/166/167,"
        "time=0/to/21/by/6",
        [
            AxisDefinition(["time"], Chunking.NONE),
            AxisDefinition(["date"], Chunking.NONE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()
    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    assert all(data[time, date, param, step] == index)


def test_access_pattern_merged_axis_non_chunked(read_only_fdb_pattern_setup) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-03,"
        "levtype=sfc,"
        "step=0,"
        "param=165/166/167,"
        "time=0/to/21/by/6",
        [
            AxisDefinition(["date", "time"], Chunking.NONE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()
    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    datetime = time + date * 4
                    assert all(data[datetime, param, step] == index)


def test_access_pattern_merged_axis_non_chunked_switched_date_time(
    read_only_fdb_pattern_setup,
) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-03,"
        "levtype=sfc,"
        "step=0,"
        "param=165/166/167,"
        "time=0/to/21/by/6",
        [
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time", "date"], Chunking.NONE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()
    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
    assert data

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    datetime = date + time * 3
                    assert all(data[param, datetime, step] == index)
