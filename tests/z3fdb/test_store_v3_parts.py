import zarr
import numpy as np

from z3fdb import (
    AxisDefinition,
    Chunking,
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
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
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
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(1)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data

    assert np.all(data[0, 0] == 0)   # date=20200101 time=0, 10u (sfc 165)
    assert np.all(data[0, 1] == 1)   # date=20200101 time=0, 10v (sfc 166)
    assert np.all(data[0, 2] == 31)  # date=20200101 time=0, u (pl 131) level=50
    assert np.all(data[0, 3] == 33)  # date=20200101 time=0, u (pl 131) level=100
    assert np.all(data[0, 4] == 32)  # date=20200101 time=0, v (pl 132) level=50
    assert np.all(data[0, 5] == 34)  # date=20200101 time=0, v (pl 132) level=100


def test_axis_check_merge_no_chunking(read_only_fdb_setup_for_sfc_pl_example) -> None:
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
        [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE), AxisDefinition(["param"], Chunking.NONE)],
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
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param", "levelist"], Chunking.NONE),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(1)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data

    assert np.all(data[0, 0] == 0)   # date=20200101 time=0, 10u (sfc 165)
    assert np.all(data[0, 1] == 1)   # date=20200101 time=0, 10v (sfc 166)
    assert np.all(data[0, 2] == 31)  # date=20200101 time=0, u (pl 131) level=50
    assert np.all(data[0, 3] == 33)  # date=20200101 time=0, u (pl 131) level=100
    assert np.all(data[0, 4] == 32)  # date=20200101 time=0, v (pl 132) level=50
    assert np.all(data[0, 5] == 34)  # date=20200101 time=0, v (pl 132) level=100


SFC_REQUEST = (
    "type=an,"
    "class=ea,"
    "domain=g,"
    "expver=0001,"
    "stream=oper,"
    "date=2020-01-01/2020-01-02,"
    "levtype=sfc,"
    "step=0,"
    "param=165/166,"
    "time=0/to/21/by/3"
)

PL_REQUEST = (
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
    "time=0/to/21/by/3"
)


def test_extend_on_axis_0(read_only_fdb_setup_for_sfc_pl_example) -> None:
    """Extension on axis 0 with SINGLE_VALUE chunking. Param axis before date+time."""
    builder = SimpleStoreBuilder(read_only_fdb_setup_for_sfc_pl_example)
    builder.add_part(
        SFC_REQUEST,
        [AxisDefinition(["param"], Chunking.SINGLE_VALUE), AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)],
        ExtractorType.GRIB,
    )
    builder.add_part(
        PL_REQUEST,
        [
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(0)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data

    # data[param_idx, date_time_idx]
    assert np.all(data[0, 0] == 0)   # 10u (sfc 165), date=20200101 time=0
    assert np.all(data[1, 0] == 1)   # 10v (sfc 166), date=20200101 time=0
    assert np.all(data[2, 0] == 31)  # u (pl 131) level=50, date=20200101 time=0
    assert np.all(data[3, 0] == 33)  # u (pl 131) level=100, date=20200101 time=0
    assert np.all(data[4, 0] == 32)  # v (pl 132) level=50, date=20200101 time=0
    assert np.all(data[5, 0] == 34)  # v (pl 132) level=100, date=20200101 time=0


def test_extend_on_axis_0_no_chunking(read_only_fdb_setup_for_sfc_pl_example) -> None:
    """Extension on axis 0 with NONE chunking on extension axis."""
    builder = SimpleStoreBuilder(read_only_fdb_setup_for_sfc_pl_example)
    builder.add_part(
        SFC_REQUEST,
        [AxisDefinition(["param"], Chunking.NONE), AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)],
        ExtractorType.GRIB,
    )
    builder.add_part(
        PL_REQUEST,
        [
            AxisDefinition(["param", "levelist"], Chunking.NONE),
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(0)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data

    # data[param_idx, date_time_idx]
    assert np.all(data[0, 0] == 0)   # 10u (sfc 165), date=20200101 time=0
    assert np.all(data[1, 0] == 1)   # 10v (sfc 166), date=20200101 time=0
    assert np.all(data[2, 0] == 31)  # u (pl 131) level=50, date=20200101 time=0
    assert np.all(data[3, 0] == 33)  # u (pl 131) level=100, date=20200101 time=0
    assert np.all(data[4, 0] == 32)  # v (pl 132) level=50, date=20200101 time=0
    assert np.all(data[5, 0] == 34)  # v (pl 132) level=100, date=20200101 time=0


def test_non_extension_axis_no_chunking(read_only_fdb_setup_for_sfc_pl_example) -> None:
    """NONE chunking on non-extension axis (date+time), SINGLE_VALUE on extension axis (param)."""
    builder = SimpleStoreBuilder(read_only_fdb_setup_for_sfc_pl_example)
    builder.add_part(
        SFC_REQUEST,
        [AxisDefinition(["date", "time"], Chunking.NONE), AxisDefinition(["param"], Chunking.SINGLE_VALUE)],
        ExtractorType.GRIB,
    )
    builder.add_part(
        PL_REQUEST,
        [
            AxisDefinition(["date", "time"], Chunking.NONE),
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(1)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data

    # data[date_time_idx, param_idx]
    assert np.all(data[0, 0] == 0)   # date=20200101 time=0, 10u (sfc 165)
    assert np.all(data[0, 1] == 1)   # date=20200101 time=0, 10v (sfc 166)
    assert np.all(data[1, 0] == 2)   # date=20200101 time=300, 10u (sfc 165)
    assert np.all(data[0, 2] == 31)  # date=20200101 time=0, u (pl 131) level=50
    assert np.all(data[0, 3] == 33)  # date=20200101 time=0, u (pl 131) level=100
    assert np.all(data[0, 4] == 32)  # date=20200101 time=0, v (pl 132) level=50
    assert np.all(data[0, 5] == 34)  # date=20200101 time=0, v (pl 132) level=100


def test_single_key_axes(read_only_fdb_setup_for_sfc_pl_example) -> None:
    """Separate date, time, param axes (no compound axes). Extension on axis 2."""
    builder = SimpleStoreBuilder(read_only_fdb_setup_for_sfc_pl_example)
    builder.add_part(
        SFC_REQUEST,
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.add_part(
        PL_REQUEST,
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(2)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data

    # data[date_idx, time_idx, param_idx]
    assert np.all(data[0, 0, 0] == 0)   # date=20200101, time=0, 10u (sfc 165)
    assert np.all(data[0, 0, 1] == 1)   # date=20200101, time=0, 10v (sfc 166)
    assert np.all(data[0, 0, 2] == 31)  # date=20200101, time=0, u (pl 131) level=50
    assert np.all(data[0, 0, 3] == 33)  # date=20200101, time=0, u (pl 131) level=100
    assert np.all(data[0, 0, 4] == 32)  # date=20200101, time=0, v (pl 132) level=50
    assert np.all(data[0, 0, 5] == 34)  # date=20200101, time=0, v (pl 132) level=100
    assert np.all(data[0, 1, 0] == 2)   # date=20200101, time=300, 10u (sfc 165)
    assert np.all(data[1, 0, 0] == 16)  # date=20200102, time=0, 10u (sfc 165)


def test_all_no_chunking(read_only_fdb_setup_for_sfc_pl_example) -> None:
    """Both axes NONE. Extension on axis 1. Single giant chunk."""
    builder = SimpleStoreBuilder(read_only_fdb_setup_for_sfc_pl_example)
    builder.add_part(
        SFC_REQUEST,
        [AxisDefinition(["date", "time"], Chunking.NONE), AxisDefinition(["param"], Chunking.NONE)],
        ExtractorType.GRIB,
    )
    builder.add_part(
        PL_REQUEST,
        [
            AxisDefinition(["date", "time"], Chunking.NONE),
            AxisDefinition(["param", "levelist"], Chunking.NONE),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(1)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data

    # data[date_time_idx, param_idx]
    assert np.all(data[0, 0] == 0)   # date=20200101 time=0, 10u (sfc 165)
    assert np.all(data[0, 1] == 1)   # date=20200101 time=0, 10v (sfc 166)
    assert np.all(data[0, 2] == 31)  # date=20200101 time=0, u (pl 131) level=50
    assert np.all(data[0, 3] == 33)  # date=20200101 time=0, u (pl 131) level=100
    assert np.all(data[0, 4] == 32)  # date=20200101 time=0, v (pl 132) level=50
    assert np.all(data[0, 5] == 34)  # date=20200101 time=0, v (pl 132) level=100
