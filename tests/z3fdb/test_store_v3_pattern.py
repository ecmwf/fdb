from numpy import random
import zarr
from pychunked_data_view.chunked_data_view import (
    AxisDefinition,
    ChunkedDataViewBuilder,
    ExtractorType,
)
from z3fdb.mapping import (
    FdbSource,
    FdbZarrArray,
    FdbZarrGroup,
    FdbZarrStore,
)

from itertools import permutations


def test_access_pattern(read_only_fdb_pattern_setup) -> None:
    builder = ChunkedDataViewBuilder(read_only_fdb_pattern_setup)
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
            AxisDefinition(["date"], True),
            AxisDefinition(["time"], True),
            AxisDefinition(["param"], True),
            AxisDefinition(["step"], True),
        ],
        ExtractorType.GRIB,
    )
    view = builder.build()

    mapping = FdbZarrStore(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(view),
                )
            ]
        )
    )
    store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
    data = store.get("data")
    assert data

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    assert all(data[date, time, param, step] == index)


def test_access_pattern_shuffled(read_only_fdb_pattern_setup) -> None:
    axis = [
        AxisDefinition(["date"], True),
        AxisDefinition(["time"], True),
        AxisDefinition(["param"], True),
        AxisDefinition(["step"], True),
    ]
    axis_values = [
        ["2020-01-01", "2020-01-02", "2020-01-03"],
        ["0000", "0600", "1200", "1800"],
        ["165", "166"],
        ["0"],
    ]
    axis_names = ["date", "time", "param", "step"]
    indices = [0, 1, 2, 3]

    index_permutations = permutations(indices)

    for index_permutation in index_permutations:
        axis_permutation = [axis[i] for i in index_permutation]

        builder = ChunkedDataViewBuilder(read_only_fdb_pattern_setup)
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
        view = builder.build()

        mapping = FdbZarrStore(
            FdbZarrGroup(
                children=[
                    FdbZarrArray(
                        name="data",
                        datasource=FdbSource(view),
                    )
                ]
            )
        )
        store = zarr.open_group(
            mapping, mode="r", zarr_format=3, use_consolidated=False
        )
        data = store.get("data")
        assert data

        print(f"Permutation: ({[axis_names[i] for i in index_permutation]})")

        def choice_random_index_permutations(i):
            return random.choice(range(len(axis_values[index_permutation[i]])))

        def compute_value(date, time, param, step):
            return step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1

        print(data.shape)
        print(data[:, 0, 0, 0])

        for date in range(0, 3):
            for time in range(0, 4):
                for param in range(0, 3):
                    for step in range(0, 1):

                        cur_index = [0, 0, 0, 0]
                        cur_index[index_permutation[0]] = date
                        cur_index[index_permutation[1]] = time
                        cur_index[index_permutation[2]] = param
                        cur_index[index_permutation[3]] = step

                        value = compute_value(date=date, time=time, param=param, step=step)

                        print(f"({date}, {time}, {param}, {step})")
                        print(f"CurIndex: {cur_index}")
                        print(f"Value: {value}")
                        print(f"Data: {data[*cur_index]}")

                        assert all(data[*cur_index] == value)


def test_access_pattern_non_chunked(read_only_fdb_pattern_setup) -> None:
    builder = ChunkedDataViewBuilder(read_only_fdb_pattern_setup)
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
            AxisDefinition(["date"], False),
            AxisDefinition(["time"], False),
            AxisDefinition(["param"], True),
            AxisDefinition(["step"], True),
        ],
        ExtractorType.GRIB,
    )
    view = builder.build()

    mapping = FdbZarrStore(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(view),
                )
            ]
        )
    )
    store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
    data = store.get("data")
    assert data

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    assert all(data[date, time, param, step] == index)


def test_access_pattern_non_chunked_mixed(read_only_fdb_pattern_setup) -> None:
    builder = ChunkedDataViewBuilder(read_only_fdb_pattern_setup)
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
            AxisDefinition(["time"], False),
            AxisDefinition(["date"], False),
            AxisDefinition(["param"], True),
            AxisDefinition(["step"], True),
        ],
        ExtractorType.GRIB,
    )
    view = builder.build()

    mapping = FdbZarrStore(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(view),
                )
            ]
        )
    )
    store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
    data = store.get("data")
    assert data

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    assert all(data[time, date, param, step] == index)


def test_access_pattern_merged_axis_non_chunked(read_only_fdb_pattern_setup) -> None:
    builder = ChunkedDataViewBuilder(read_only_fdb_pattern_setup)
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
            AxisDefinition(["date", "time"], False),
            AxisDefinition(["param"], True),
            AxisDefinition(["step"], True),
        ],
        ExtractorType.GRIB,
    )
    view = builder.build()

    mapping = FdbZarrStore(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(view),
                )
            ]
        )
    )
    store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
    data = store.get("data")
    assert data

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    datetime = time + date * 4
                    assert all(data[datetime, param, step] == index)


def test_access_pattern_merged_axis_non_chunked(read_only_fdb_pattern_setup) -> None:
    builder = ChunkedDataViewBuilder(read_only_fdb_pattern_setup)
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
            AxisDefinition(["param"], True),
            AxisDefinition(["time", "date"], False),
            AxisDefinition(["step"], True),
        ],
        ExtractorType.GRIB,
    )
    view = builder.build()

    mapping = FdbZarrStore(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(view),
                )
            ]
        )
    )
    store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
    data = store.get("data")
    assert data

    for date in range(0, 3):
        for time in range(0, 4):
            for param in range(0, 3):
                for step in range(0, 1):
                    index = step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1
                    datetime = date + time * 3
                    assert all(data[param, datetime, step] == index)
