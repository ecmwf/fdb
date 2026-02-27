# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import enum
import pathlib

import chunked_data_view_bindings.chunked_data_view_bindings as pdv

from pychunked_data_view.exceptions import MarsRequestFormattingError, InternalError

pdv.init_bindings()


class Chunking(enum.Enum):
    """Defines how a axis will be chunked

    Attributes:
        NONE: Axis will not be chunked, accessing any value from this axis will load all values.
        SINGLE_VALUE: Axis will be chunked. One chunk per value.
    """

    NONE = enum.auto()
    SINGLE_VALUE = enum.auto()


class AxisDefinition:
    @staticmethod
    def _translate_chunking(
        chunking: Chunking,
    ) -> pdv.AxisDefinition.NoChunking | pdv.AxisDefinition.IndividualChunking:
        if chunking == Chunking.NONE:
            return pdv.AxisDefinition.NoChunking()
        elif chunking == Chunking.SINGLE_VALUE:
            return pdv.AxisDefinition.IndividualChunking()
        else:
            raise InternalError()

    def __init__(self, keys: list[str], chunking: Chunking):
        """Defines which axis from a MARS Request form an axis in the Zarr array.

        Also defines if the data is to be chunked.

        Args:
            keys(list of str): mars keys that for this axis.
            chunking ( Chunking): Define how this axis shall be chunked
        """
        self._obj = pdv.AxisDefinition(
            keys=keys, chunking=self._translate_chunking(chunking)
        )

    @property
    def keys(self) -> list[str]:
        return self._obj.keys

    @keys.setter
    def keys(self, keys: list[str]) -> None:
        self._obj.keys = keys

    @property
    def chunking(self) -> Chunking:
        chunking = self._obj.chunking
        if isinstance(chunking, pdv.AxisDefinition.NoChunking):
            return Chunking.NONE
        elif isinstance(chunking, pdv.AxisDefinition.IndividualChunking):
            return Chunking.SINGLE_VALUE
        else:
            raise InternalError()

    @chunking.setter
    def chunking(self, chunking: Chunking) -> None:
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
        mars_request_key_values: str,
        axes: list[AxisDefinition],
        extractor_type: ExtractorType,
    ):
        self._obj.add_part(
            mars_request_key_values, [ax._obj for ax in axes], extractor_type.value
        )

    def extend_on_axis(self, axis: int):
        self._obj.extend_on_axis(axis)

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
