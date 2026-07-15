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
    """Defines how a axis will be chunked

    Attributes:
        NONE: Axis will not be chunked, accessing any value from this axis will load all values.
        SINGLE_VALUE: Axis will be chunked. One chunk per value.
    """

    NONE = enum.auto()
    SINGLE_VALUE = enum.auto()

    @enum.nonmember
    @dataclass(frozen=True)
    class IndividualChunk:
        chunkShape: int


class AxisDefinition:
    @staticmethod
    def _translate_chunking(
        chunking: Chunking | Chunking.IndividualChunk,
    ) -> (
        pdv.AxisDefinition.NoChunking
        | pdv.AxisDefinition.SingleValueChunking
        | pdv.AxisDefinition.IndividualChunking
    ):
        if isinstance(chunking, Chunking.IndividualChunk):
            return pdv.AxisDefinition.IndividualChunking(chunking.chunkShape)
        elif chunking is Chunking.NONE:
            return pdv.AxisDefinition.NoChunking()
        elif chunking is Chunking.SINGLE_VALUE:
            return pdv.AxisDefinition.SingleValueChunking()
        else:
            raise InternalError()

    def __init__(
        self,
        keys: list[str],
        chunking: Chunking | Chunking.IndividualChunk,
        dim_name: str | None = None,
    ):
        """Defines which axis from a MARS Request form an axis in the Zarr array.

        Also defines if the data is to be chunked.

        Args:
            keys(list of str): mars keys that form this axis.
            chunking (Chunking): Define how this axis shall be chunked.
            dim_name (str | None): Optional dimension name for xarray compatibility.
                If not provided, the name is auto-derived as the keys joined by "_"
                (e.g. ``["date", "time"]`` → ``"date_time"``).
        """
        self._obj = pdv.AxisDefinition(
            keys=keys, chunking=self._translate_chunking(chunking)
        )
        self._dim_name: str = dim_name if dim_name is not None else "_".join(keys)

    @property
    def dim_name(self) -> str:
        return self._dim_name

    @dim_name.setter
    def dim_name(self, name: str) -> None:
        self._dim_name = name

    @property
    def keys(self) -> list[str]:
        return self._obj.keys

    @keys.setter
    def keys(self, keys: list[str]) -> None:
        self._obj.keys = keys

    @property
    def chunking(self):
        chunking = self._obj.chunking
        if isinstance(chunking, pdv.AxisDefinition.NoChunking):
            return Chunking.NONE
        elif isinstance(chunking, pdv.AxisDefinition.SingleValueChunking):
            return Chunking.SINGLE_VALUE
        elif isinstance(chunking, pdv.AxisDefinition.IndividualChunking):
            return Chunking.IndividualChunk(chunking.chunk_shape())
        else:
            raise InternalError()

    @chunking.setter
    def chunking(self, chunking: Chunking | Chunking.IndividualChunk) -> None:
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

    def fillValue(self):
        return self._obj.fillValue()


class ExtractorType(enum.Enum):
    """Suported data extractors.

    Defines what storage format the caller expects to be stored in FDB.
    """

    GRIB = pdv.ExtractorType.GRIB
    """Extract data from GRIB"""


class ChunkedDataViewBuilder:
    def __init__(self, fdb_config_file: pathlib.Path | None):
        self._obj = pdv.ChunkedDataViewBuilder(fdb_config_file)
        self._parts_axes: list[list[AxisDefinition]] = []

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
        self._parts_axes.append(list(axes))

    def extend_on_axis(self, axis: int):
        self._obj.extend_on_axis(axis)

    def fill_value(self, value: float):
        self._obj.fill_value(value)

    def dim_names(self) -> list[str | None]:
        """Return dimension names for all zarr axes, derived from the first added part.

        The last entry is always ``None`` to represent the implicit field-values
        dimension (spatial data points within each GRIB field).

        Returns:
            list[str | None]: One entry per zarr dimension; final entry is ``None``.
        """
        if not self._parts_axes:
            return []
        return [ax.dim_name for ax in self._parts_axes[0]] + [None]

    def build(self):
        try:
            return ChunkedDataView(self._obj.build())
        except RuntimeError as re:
            exception_msg = str(re)
            if "StreamParser::next" in exception_msg:
                raise MarsRequestFormattingError(
                    exception_msg + "\n Did the MARS request end in a comma?"
                )
            elif "MarsParser::parseVerb" in exception_msg:
                raise MarsRequestFormattingError(
                    exception_msg + "\n Did you miss a comma between keys?"
                )
            elif "Cannot match" in exception_msg:
                raise MarsRequestFormattingError(
                    exception_msg + "\n Did you misspell a MARS key?"
                )
            else:
                raise re
