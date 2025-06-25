# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.


import zarr
import zarr.storage

from pychunked_data_view.chunked_data_view import AxisDefinition, ChunkedDataViewBuilder, ExtractorType
from z3fdb.mapping import (
    FdbSource,
    FdbZarrArray,
    FdbZarrGroup,
    FdbZarrStore,
)


def test_zarr_use_spec_v2(read_only_fdb_setup) -> None:
    assert zarr.config.get("default_zarr_format") == 3


# def test_make_anemoi_dataset_like_view(
#     read_only_fdb_setup_for_anemoi_recipe_example, data_path
# ) -> None:
#     example_recipe = data_path / "anemoi-recipes" / "example.yaml"
#     fdb_config_path = read_only_fdb_setup_for_anemoi_recipe_example
#
#     recipe = yaml.safe_load(example_recipe.read_text())
#     mapping = make_anemoi_dataset_like_view(recipe=recipe, fdb_config=fdb_config_path)
#     store = zarr.open_group(mapping, mode="r", zarr_format=3)
#     data = store.get("data")
#
#     assert data
#     query = copy.deepcopy(data[:, :, :])
#     assert np.array_equal(query, data[:, :, :])
#
#
# @pytest.mark.asyncio
# async def test_make_anemoi_dataset_like_view_2(read_only_fdb_setup) -> None:
#     config_path = read_only_fdb_setup
#     recipe_path = data_path / "recipes" / "example.yaml"
#     recipe = yaml.safe_load(recipe_path.read_text())
#
#     mapping = make_anemoi_dataset_like_view(recipe=recipe)
#     metadata_bytes = await mapping.get(".zmetadata")
#
#     assert metadata_bytes
#     metadata_string = from_cpu_buffer(metadata_bytes)
#
#     assert "metadata" in metadata_string
#     assert len(metadata_string["metadata"]) > 0


# @pytest.mark.asyncio
# def test_make_view(read_only_fdb_setup) -> None:
#     mapping = FdbZarrStore(
#         FdbZarrGroup(
#             children=[
#                 FdbZarrArray(
#                     name="data",
#                     datasource=FdbSource(
#                         request=[
#                             Request(
#                                 request={
#                                     "date": np.arange(
#                                         np.datetime64("2020-01-01"),
#                                         np.datetime64("2020-01-03"),
#                                     ),
#                                     "time": ["00", "06", "12", "18"],
#                                     "class": "ea",
#                                     "domain": "g",
#                                     "expver": "0001",
#                                     "stream": "oper",
#                                     "type": "an",
#                                     "step": "0",
#                                     "levtype": "sfc",
#                                     "param": ["10u", "10v"],
#                                 },
#                                 chunk_axis=ChunkAxisType.DateTime,
#                             )
#                         ]
#                     ),
#                 )
#             ]
#         )
#     )
#     store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
#


def test_access(read_only_fdb_setup) -> None:
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
        [AxisDefinition(["date", "time"], True), AxisDefinition(["param"], True)],
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
    print(data[:, :])


# @pytest.mark.asyncio
# async def test_access_listing_test(read_only_fdb_setup) -> None:
#     mapping = FdbZarrStore(
#         FdbZarrGroup(
#             children=[
#                 FdbZarrGroup(name="xxx"),
#                 FdbZarrGroup(
#                     name="retrograde",
#                     children=[
#                         FdbZarrArray(name="value1", datasource=ConstantValue(6)),
#                         FdbZarrArray(
#                             name="value2",
#                             datasource=ConstantValueField(
#                                 value=123, shape=(100, 10, 1000), chunks=(100, 10, 100)
#                             ),
#                         ),
#                         FdbZarrArray(
#                             name="dates",
#                             datasource=make_dates_source(
#                                 np.datetime64("1979-01-01T00:00:00", "s"),
#                                 np.datetime64("2024-01-01T00:00:00", "s"),
#                                 np.timedelta64(6, "h"),
#                             ),
#                         ),
#                     ],
#                 ),
#             ]
#         )
#     )
#
#     store = zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)
#     data = store.get("retrograde/value1")
#     assert data[0] == 6
#
#     data2 = store.get("retrograde/value2")
#     assert np.all(data2[:] == 123)
#
