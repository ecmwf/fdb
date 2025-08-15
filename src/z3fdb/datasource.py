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

import numpy as np
from zarr.core.buffer.cpu import Buffer as CpuBuffer
from pychunked_data_view import ChunkedDataView

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
        chunked_data_view: ChunkedDataView,
    ) -> None:
        self._chunked_data_view = chunked_data_view

        self._shape = self._chunked_data_view.shape()
        self._chunks = self._chunked_data_view.chunkShape()
        self._chunks_per_dimension = self._chunked_data_view.chunks()

    @override
    def create_dot_zarr_json(self) -> CpuBuffer:
        return to_cpu_buffer(
            DotZarrArrayJson(
                shape=self._shape,
                chunk_grid=ChunkGridMetadata(chunks=self._chunks),
                data_type="float32",
                fill_value=-1.0
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

    def chunks(self) -> tuple[int, ...]:
        return self._chunks_per_dimension

    def __getitem__(self, key: tuple[int, ...]) -> CpuBuffer:
        if len(key) != len(self._shape):
            raise KeyError
        if any(
            k < 0 or k >= limit for k, limit in zip(key, self._chunks_per_dimension)
        ):
            raise KeyError
        return CpuBuffer.from_bytes(self._chunked_data_view.at(key))
