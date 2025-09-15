import zarr
import numpy as np

from z3fdb import (
    AxisDefinition,
    ExtractorType,
    SimpleStoreBuilder,
)


def test_axis_check_merge(read_only_fdb_setup_for_sfc_pl_example) -> None:
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
        "param=165/166,"
        "time=0/to/21/by/3",
        [AxisDefinition(["date", "time"], True), AxisDefinition(["param"], True)],
        ExtractorType.GRIB,
    )
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/2020-01-02,"
        "levtype=pl,"
        "step=0,"
        "param=131/132,"
        "levelist=50/100,"
        "time=0/to/21/by/3",
        [
            AxisDefinition(["date", "time"], True),
            AxisDefinition(["param", "levelist"], True),
        ],
        ExtractorType.GRIB,
    )
    builder.extendOnAxis(1)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data

    assert np.all(data[0, 0] == 0)
    assert np.all(data[0, 1] == 1)
    assert np.all(data[0, 2] == 31)
    assert np.all(data[0, 3] == 33)
    assert np.all(data[0, 4] == 32)
    assert np.all(data[0, 5] == 34)
