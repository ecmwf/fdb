# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import json
import logging
import re
import pathlib
from collections.abc import Buffer
from typing import AsyncIterator, Iterable

import numpy as np
import pyfdb

from zarr.abc import store
from zarr.core.buffer import default_buffer_prototype
from zarr.core.buffer.core import Buffer as AbstractBuffer
from zarr.core.buffer.core import BufferPrototype
from zarr.core.buffer.cpu import Buffer as CpuBuffer
from zarr.core.common import BytesLike

from z3fdb.datasource import (
    FdbSource,
)
from z3fdb.error import ZfdbError
from z3fdb.request import ChunkAxisType, Request
from z3fdb.zarr import FdbZarrArray, FdbZarrGroup

from pychunked_data_view import ChunkedDataViewBuilder, AxisDefinition, ExtractorType

log = logging.getLogger(__name__)


class FdbZarrStore(store.Store):
    """Provide access to FDB with a MutableMapping.

    .. note:: This is an experimental feature.

    Requires the `pyFDB <https://redis-py.readthedocs.io/>`_
    package to be installed.

    Parameters
    ----------
    """

    def __init__(self, child: FdbZarrGroup | FdbZarrArray):
        super().__init__(read_only=True)

        self._child = child
        self._known_paths = self._build_paths(self._child)
        self._zmetadata = self._consolidate()

    def _build_paths(self, item, parent_path=None) -> list[str]:
        path = f"{parent_path}/{item.name}" if parent_path else item.name
        files = [f"{path}/{f}" if path != "" else f for f in item.paths()]

        if isinstance(item, FdbZarrGroup):
            for child in item.children:
                files += self._build_paths(child, path)

        return files

    def _consolidate(self):
        consolidated_metatdata = {"metadata": {}}

        ENDINGS = ["zarr.json"]

        filtered_paths = (
            path
            for path in self._known_paths
            for key in ENDINGS
            if path is not None and path.endswith(key)
        )

        for path in filtered_paths:
            keys = path.split("/")
            info = self._child[*keys]

            if info is None:
                continue

            consolidated_metatdata["metadata"].update(
                {path: json.loads(info.to_bytes())}
            )

        # Create Buffer
        return CpuBuffer.from_bytes(json.dumps(consolidated_metatdata).encode("utf-8"))

    async def __getitem__(self, key) -> AbstractBuffer | None:
        if key == ".zmetadata":
            return self._zmetadata
        if key == "zarr.json":
            return self._child._metadata

        keys = key.split("/")
        return self._child[*keys]

    def __iter__(self):
        yield from iter(self._known_paths)

    def __len__(self):
        return len(self._known_paths)

    def __setitem__(self, _k, _v):
        # TODO(kkratz): should raise proper exception
        pass

    def __delitem__(self, _k):
        # TODO(kkratz): should raise proper exception
        pass

    def __contains__(self, key) -> bool:
        return key in self._known_paths

    def __eq__(self, value: object) -> bool:
        return self._child == value._child and self._known_paths == value._known_paths

    async def get(
        self,
        key: str,
        prototype: BufferPrototype = default_buffer_prototype(),
        byte_range: store.ByteRequest | None = None,
    ) -> Buffer | None:
        return await self.__getitem__(key)

    def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, store.ByteRequest | None]],
    ) -> list[Buffer | None]:
        pass

    async def exists(self, key: str) -> bool:
        return key in self._known_paths

    @property
    def supports_writes(self) -> bool:
        return False

    async def set(self, key: str, value: AbstractBuffer) -> None:
        raise NotImplementedError()

    async def set_if_not_exists(self, key: str, value: AbstractBuffer) -> None:
        raise NotImplementedError()

    async def _set_many(self, values: Iterable[tuple[str, AbstractBuffer]]) -> None:
        return await super()._set_many(values)

    @property
    def supports_deletes(self) -> bool:
        return False

    async def delete(self, key: str) -> None:
        raise NotImplementedError()

    @property
    def supports_partial_writes(self) -> bool:
        return False

    async def set_partial_values(
        self, key_start_values: Iterable[tuple[str, int, BytesLike]]
    ) -> None:
        raise NotImplementedError()

    @property
    def supports_listing(self) -> bool:
        return True

    async def list(self) -> AsyncIterator[str]:
        for i in self._known_paths:
            yield i

    def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        return self._child.list_prefix(prefix)

    def list_dir(self, prefix: str) -> AsyncIterator[str]:
        return self._build_paths(self._child, parent_path=prefix)


def extract_mars_requests_from_recipe(recipe: dict):
    required_keys = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "type": "an",
        "step": "0",
    }
    base_request = dict()
    # NOTE(kkratz): There is more available than just common.mars_request
    if "common" in recipe and "mars_request" in recipe["common"]:
        base_request = recipe["common"]["mars_request"]

    if "dates" not in recipe:
        raise ZfdbError("Expected 'dates' in recipe")

    start_date = recipe["dates"]["start"].split("T")[0]
    end_date = recipe["dates"]["end"].split("T")[0]
    dates = [
        str(d).replace("-", "")
        for d in np.arange(
            np.datetime64(start_date, "D"), np.datetime64(end_date, "D") + 1
        )
    ]

    # TODO(kkratz): There is code in anemoi.utils to convert the textual representation to a timedelta64
    # see: anemoi.utils.dates.as_timedelta(...)
    def parse_frequency(s: str):
        match = re.match(r"^(\d+)([d|h|m|s])$", s)
        if not match:
            raise ZfdbError(f"Invalid frequency str in recipe ('{s}')")
        return int(match.group(1)), match.group(2)

    frequency = parse_frequency(recipe["dates"]["frequency"])
    num_times = np.timedelta64(1, "D") // np.timedelta64(frequency[0], frequency[1])
    times = [
        str(
            np.datetime64("2000-01-01T00", "h")
            + x * np.timedelta64(frequency[0], frequency[1])
        ).split("T")[1]
        for x in range(num_times)
    ]

    # recipes do support more than joins under input but for the sake of example we require
    # this to be present for now
    if "input" not in recipe or "join" not in recipe["input"]:
        raise ZfdbError("Expected 'input.join' in recipe")

    #  for now we only act on "mars" inputs, they need to be joined along the date-time axis
    inputs = recipe["input"]["join"]
    requests = [
        {"date": dates, "time": times} | required_keys | base_request | src["mars"]
        for src in inputs
        if "mars" in src
    ]
    for r in requests:
        r.pop("grid", None)
    return requests


def make_anemoi_dataset_like_view(
    *,
    fdb_config: pathlib.Path | None = None,
    recipe: dict,
) -> FdbZarrStore:
    # get common mars request part
    mars_requests = extract_mars_requests_from_recipe(recipe)

    def to_str(d):
        return ",".join([f"{k}={v}" for k, v in d.items()])

    builder = ChunkedDataViewBuilder(fdb_config)
    for req in mars_requests:
        for k,v in req.items():
            if isinstance(v, list):
                req[k] = "/".join([str(x) for x in v])
        s = to_str(req)
        if req["levtype"] == "sfc":
            builder.add_part(
                s, [AxisDefinition(["date", "time"], True), AxisDefinition(["param"], True)], ExtractorType.GRIB
            )
        elif req["levtype"] == "pl":
            builder.add_part(
                s, [AxisDefinition(["date", "time"], True), AxisDefinition(["param", "levelist"], True)], ExtractorType.GRIB
            )
        else:
            raise ZfdbError(f"Unexpected levtype encountered {req['levtype']}")
    builder.extendOnAxis(1)
    chunked_data_view = builder.build()

    return FdbZarrStore(
        FdbZarrGroup(
            children=[
                # FdbZarrArray(
                #     name="dates",
                #     datasource=make_dates_source(dates=mars_requests[0]["date"], times=mars_requests[0]["time"]),
                # ),
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(chunked_data_view),
                ),
            ]
        )
    )


def make_forecast_data_view(
    *,
    request: Request | list[Request],
) -> FdbZarrStore:
    requests = request if isinstance(request, list) else [request]
    # if len(requests) > 1 and not all(
    #     map(lambda x: x[0].matches_on_time_axis(x[1]), zip(requests[:-1], requests[1:]))
    # ):
    #     raise ZfdbError("Requests are not matching on time axis")

    return FdbZarrStore(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(request=requests),
                ),
            ]
        )
    )
