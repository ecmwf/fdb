import zarr
from pychunked_data_view.chunked_data_view import AxisDefinition, ChunkedDataViewBuilder, ExtractorType
from z3fdb.mapping import (
    FdbSource,
    FdbZarrArray,
    FdbZarrGroup,
    FdbZarrStore,
)

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
        [AxisDefinition(["date"], True), AxisDefinition(["time"], True), AxisDefinition(["param"], True), AxisDefinition(["step"], True)],
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
                    assert(all(data[date, time, param, step] == index))
