# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from collections.abc import Collection, Mapping
from dataclasses import dataclass
import enum
import pathlib
from typing import TypeAlias

import chunked_data_view_bindings.chunked_data_view_bindings as pdv

from pychunked_data_view.exceptions import MarsRequestFormattingError, InternalError

# Init the bindings (eckit initialization)
pdv.init_bindings()


MarsSelection: TypeAlias = Mapping[
    str, str | int | float | Collection[str | int | float]
]


# Mapping functionality for MarsSelection
def _mars_selection_to_string(request: MarsSelection) -> str:
    parts = []
    for key, value in request.items():
        if isinstance(value, (str, int, float)):
            joined = str(value)
        else:
            joined = "/".join(str(v) for v in value)
        parts.append(f"{key}={joined}")
    return ",".join(parts)


class Chunking(enum.Enum):
    """Defines how an axis will be chunked.

    Attributes:
        WHOLE_AXIS: The entire axis is a single chunk; accessing any value loads all values on that axis.
        SINGLE_VALUE: Each value along the axis is its own chunk.
        FixedSizeChunk: Groups every ``chunkShape`` consecutive values into one chunk.
    """

    WHOLE_AXIS = enum.auto()
    SINGLE_VALUE = enum.auto()

    @enum.nonmember
    @dataclass(frozen=True)
    class FixedSizeChunk:
        chunkShape: int


class AxisDefinition:
    @staticmethod
    def _translate_chunking(
        chunking: Chunking | Chunking.FixedSizeChunk,
    ) -> (
        pdv.AxisDefinition.WholeAxisChunking
        | pdv.AxisDefinition.SingleValueChunking
        | pdv.AxisDefinition.FixedSizeChunking
    ):
        if isinstance(chunking, Chunking.FixedSizeChunk):
            return pdv.AxisDefinition.FixedSizeChunking(chunking.chunkShape)
        elif chunking is Chunking.WHOLE_AXIS:
            return pdv.AxisDefinition.WholeAxisChunking()
        elif chunking is Chunking.SINGLE_VALUE:
            return pdv.AxisDefinition.SingleValueChunking()
        else:
            raise InternalError()

    def __init__(self, keys: list[str], chunking: Chunking | Chunking.FixedSizeChunk):
        """Defines which axis from a MARS Request form an axis in the Zarr array.

        Also defines if the data is to be chunked.

        Args:
            keys(list of str): mars keys that for this axis.
            chunking ( Chunking): Define how this axis shall be chunked
        """
        self._obj = pdv.AxisDefinition(keys=keys, chunking=self._translate_chunking(chunking))

    @property
    def keys(self) -> list[str]:
        return self._obj.keys

    @keys.setter
    def keys(self, keys: list[str]) -> None:
        self._obj.keys = keys

    @property
    def chunking(self):
        chunking = self._obj.chunking
        if isinstance(chunking, pdv.AxisDefinition.WholeAxisChunking):
            return Chunking.WHOLE_AXIS
        elif isinstance(chunking, pdv.AxisDefinition.SingleValueChunking):
            return Chunking.SINGLE_VALUE
        elif isinstance(chunking, pdv.AxisDefinition.FixedSizeChunking):
            return Chunking.FixedSizeChunk(chunking.chunk_shape())
        else:
            raise InternalError()

    @chunking.setter
    def chunking(self, chunking: Chunking | Chunking.FixedSizeChunk) -> None:
        self._obj.chunking = self._translate_chunking(chunking)


class ChunkedDataView:
    def __init__(self, obj: pdv.ChunkedDataView):
        self._obj = obj

    def at(self, index: list[int] | tuple[int, ...]):
        return self._obj.at(index)

    def chunkShape(self):
        return self._obj.chunk_shape()

    def chunks(self):
        return self._obj.chunks()

    def shape(self):
        return self._obj.shape()

    def fill_missing_value(self):
        return self._obj.fill_missing_value()


class ExtractorType(enum.Enum):
    """Suported data extractors.

    Defines what storage format the caller expects to be stored in FDB.
    """

    GRIB = pdv.ExtractorType.GRIB
    """Extract data from GRIB"""


class ChunkedDataViewBuilder:
    def __init__(self, fdb_config_file: pathlib.Path | None):
        self._obj = pdv.ChunkedDataViewBuilder(fdb_config_file)

    def add_part(
        self,
        mars_request: MarsSelection,
        axes: list[AxisDefinition],
        extractor_type: ExtractorType,
    ):
        self._obj.add_part(
            _mars_selection_to_string(mars_request),
            [ax._obj for ax in axes],
            extractor_type.value,
        )

    def extend_on_axis(self, axis: int):
        self._obj.extend_on_axis(axis)

    def fill_missing_value(self, value: float):
        self._obj.fill_missing_value(value)

    def build(self):
        try:
            return ChunkedDataView(self._obj.build())
        except RuntimeError as re:
            exception_msg = str(re)
            if "StreamParser::next" in exception_msg:
                raise MarsRequestFormattingError(exception_msg + "\n Did the MARS request end in a comma?")
            elif "MarsParser::parseVerb" in exception_msg:
                raise MarsRequestFormattingError(exception_msg + "\n Did you miss a comma between keys?")
            elif "Cannot match" in exception_msg:
                raise MarsRequestFormattingError(exception_msg + "\n Did you misspell a MARS key?")
            else:
                raise re
