import logging

import zarr

from z3fdb import (
    AxisDefinition,
    ExtractorType,
    SimpleStoreBuilder,
)

logging.basicConfig(level=logging.DEBUG)


def test_random_axis_retrieval(read_only_fdb_pattern_setup) -> None:
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
            AxisDefinition(["date"], True),
            AxisDefinition(["time"], True),
            AxisDefinition(["param"], True),
            AxisDefinition(["step"], True),
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
