# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""Datasources

Contains implementations of datasources and factory functions for crating them.
"""

import itertools
import logging
import math
from functools import cache
from typing import override

import eccodes
import numpy as np
import pyfdb
import pygribjump
from zarr.core.buffer.cpu import Buffer as CpuBuffer

from .error import ZfdbError
from .request import Request, into_mars_request_dict
from .zarr import (
    ChunkGridMetadata,
    ZarrChunkedDataView,
    DotZarrArrayJson,
    to_cpu_buffer,
)

log = logging.getLogger(__name__)


class ConstantValue(ZarrChunkedDataView):
    """
    This DataSource represents a single constant value.
    Provided for testing.
    """

    def __init__(self, value: int):
        self._value = value

    @override
    def create_dot_zarr_json(self) -> CpuBuffer:
        return to_cpu_buffer(
            DotZarrArrayJson(
                shape=(1,),
                chunk_grid=ChunkGridMetadata(chunks=(1,)),
                data_type="int32",
                fill_value=self._value,
            )
        )

    def chunks(self) -> tuple[int]:
        return (0,)

    def __getitem__(self, _) -> CpuBuffer:
        # This tells zarr to use the fill value
        a = CpuBuffer.from_bytes(np.array([self._value]).tobytes())

    def __contains__(self, key) -> bool:
        return key == "0"


class ConstantValueField(ZarrChunkedDataView):
    """
    This DataSource represents n-dimensional data with a uniform constant value.
    Provided for testing.
    """

    def __init__(
        self, value: int, shape: tuple[int, ...], chunks: tuple[int, ...]
    ) -> None:
        self._value = value
        if len(shape) != len(chunks):
            raise ZfdbError()
        self._shape = shape
        self._chunks = chunks
        self._chunk_counts = [
            math.ceil(s / c) for s, c in zip(self._shape, self._chunks)
        ]
        self._data = np.full(
            shape=(self._chunks), fill_value=self._value, dtype=np.int32, order="C"
        )

    @override
    def create_dot_zarr_json(self) -> CpuBuffer:
        return to_cpu_buffer(
            DotZarrArrayJson(
                shape=self._shape,
                chunk_grid=ChunkGridMetadata(chunks=self._chunks),
                data_type="int32",
                fill_value=self._value,
            )
        )

    def chunks(self) -> tuple[int]:
        return (0,)

    def __getitem__(self, _) -> CpuBuffer:
        # This tells zarr to use the fill value
        return CpuBuffer.from_bytes(self._data.tobytes())

    def __contains__(self, key) -> bool:
        return False


class NDarraySource(ZarrChunkedDataView):
    """
    Uses a numpy.ndarray as backend.
    """

    def __init__(self, array: np.ndarray) -> None:
        self._array = array

    @override
    def create_dot_zarr_json(self) -> CpuBuffer:
        return to_cpu_buffer(
            DotZarrArrayJson(
                shape=self._array.shape,
                chunk_grid=ChunkGridMetadata(self._array.shape),
                data_type="int32",
            )
        )

    def chunks(self) -> tuple[int, ...]:
        return (1,) * len(self._array.shape)

    @cache
    def __getitem__(self, key: tuple[int, ...]) -> CpuBuffer:
        # TODO(kkratz): Currently duplicates memory used because of the caced copy of bytes,
        # but zarr does not work on a memoryview
        if len(key) != self._array.ndim:
            raise KeyError
        if any(x != 0 for x in key):
            raise KeyError
        return CpuBuffer.from_array_like(self._array)

    def __contains__(self, key) -> bool:
        if len(key) != self._array.ndim:
            return False
        if any(x != 0 for x in key):
            return False
        return True


class FdbSource(ZarrChunkedDataView):
    """
    Uses FDB as a backend.
    Data is retrieved from FDB and assembled on each access.
    """

    def __init__(
        self,
        *,
        request: Request | list[Request],
    ) -> None:
        if isinstance(request, Request):
            self._requests = [request]
        else:
            self._requests = request

        log.debug(f"Building view from requests: {[(r[0]) for r in self._requests]}")
        streams = [self._fdb.retrieve(r[0]) for r in self._requests]
        if any([x.size() == 0 for x in streams]):
            raise ZfdbError(
                "No data found for at least one of the MARS requests defining the view."
            )
        messages = itertools.chain.from_iterable(
            [eccodes.StreamReader(s) for s in streams]
        )

        field_count = 0
        self._field_names = []
        field_size = None
        for msg in messages:
            field_count += 1
            self._field_names.append(
                {"level": msg.get("level"), "name": msg.get("shortName")}
            )
            this_field_size = msg.get("numberOfDataPoints")
            if not field_size:
                field_size = this_field_size
            elif field_size != this_field_size:
                raise ZfdbError(
                    f"Found different field sizes {field_size} and {this_field_size}"
                )

        # TODO(kkratz): This needs to be made generic
        num_chunks = len(self._requests[0].chunk_axis())
        self._shape = (num_chunks, field_count, int(1), field_size)
        self._chunks = (1, field_count, 1, field_size)
        self._chunks_per_dimension = tuple(
            [math.ceil(a / b) for (a, b) in zip(self._shape, self._chunks)]
        )

    @override
    def create_dot_zarr_json(self) -> CpuBuffer:
        return to_cpu_buffer(
            DotZarrArrayJson(
                shape=self._shape,
                chunk_grid=ChunkGridMetadata(chunks=self._chunks),
                data_type="float32",
            )
        )

    def __contains__(self, key: tuple[int, ...]) -> bool:
        if len(key) != len(self._shape):
            return False
        if any(
            k < 0 or k >= limit for k, limit in zip(key, self._chunks_per_dimension)
        ):
            return False
        return True



def make_dates_source(
    start: np.datetime64, stop: np.datetime64, interval: np.timedelta64
) -> NDarraySource:
    dates_count = 1 + (stop - start) // interval
    array = np.zeros(dates_count, dtype="datetime64[s]")
    for idx in range(0, dates_count):
        array[idx] = start + interval * idx
    return NDarraySource(array)


def make_lat_long_sources(
    fdb: pyfdb.FDB,
    request: dict,
) -> tuple[NDarraySource, NDarraySource]:
    request = into_mars_request_dict(request)
    msg = fdb.retrieve(request)
    content = msg.read()
    gid = eccodes.codes_new_from_message(bytes(content))
    length = int(eccodes.codes_get(gid, "numberOfDataPoints"))
    lat = np.ndarray(
        length, dtype="float64", buffer=eccodes.codes_get_double_array(gid, "latitudes")
    )
    lon = np.ndarray(
        length,
        dtype="float64",
        buffer=eccodes.codes_get_double_array(gid, "longitudes"),
    )
    eccodes.codes_release(gid)

    return NDarraySource(lat), NDarraySource(lon)

