# SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import enum
import pathlib
import warnings
from collections.abc import Collection, Mapping
from typing import TypeAlias

import numpy

import chunked_data_view_bindings as pdv
from chunked_data_view_bindings import (  # noqa: E402
    GribExtractorError as GribExtractorError,
    GribJumpExtractorError as GribJumpExtractorError,
    has_gribjump_extractor as has_gribjump_extractor,
)
from pychunked_data_view.exceptions import InternalError, MarsRequestFormattingError

MarsSelection: TypeAlias = Mapping[str, str | int | float | Collection[str | int | float]]


def _mars_selection_to_string(request: MarsSelection) -> str:
    """Serialise a :class:`MarsSelection` dict to a ``key=value,...`` MARS string.

    Args:
        request (MarsSelection): MARS key-value mapping to serialise.

    Returns:
        str: Comma-separated ``key=value`` pairs, with multi-valued entries
            joined by ``/``.
    """
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
        FixedSizeChunk: Groups every ``chunk_shape`` consecutive values into one chunk.
    """

    WHOLE_AXIS = enum.auto()
    SINGLE_VALUE = enum.auto()

    @enum.nonmember
    class FixedSizeChunk:
        def __init__(self, chunk_shape: int) -> None:
            assert chunk_shape > 0, "The supplied chunk shape needs to be positive"
            self.chunk_shape = chunk_shape


class AxisDefinition:
    """Maps one or more MARS keys to a single zarr array axis with a given chunking strategy."""

    @staticmethod
    def _translate_chunking(
        chunking: Chunking | Chunking.FixedSizeChunk,
    ) -> (
        pdv.AxisDefinition.WholeAxisChunking
        | pdv.AxisDefinition.SingleValueChunking
        | pdv.AxisDefinition.FixedSizeChunking
    ):
        """Convert a Python :class:`Chunking` value to the corresponding C++ binding type.

        Args:
            chunking (~pychunked_data_view.Chunking | ~pychunked_data_view.Chunking.FixedSizeChunk):
                Chunking strategy to translate.

        Returns:
            The matching ``pdv.AxisDefinition`` chunking object.

        Raises:
            TypeError: If *chunking* is not a recognised :class:`Chunking` value.
        """
        if isinstance(chunking, Chunking.FixedSizeChunk):
            return pdv.AxisDefinition.FixedSizeChunking(chunking.chunk_shape)
        elif chunking is Chunking.WHOLE_AXIS:
            return pdv.AxisDefinition.WholeAxisChunking()
        elif chunking is Chunking.SINGLE_VALUE:
            return pdv.AxisDefinition.SingleValueChunking()
        else:
            raise TypeError(
                f"chunking must be Chunking.WHOLE_AXIS, Chunking.SINGLE_VALUE, or an instance of "
                f"Chunking.FixedSizeChunk, got {type(chunking).__qualname__!r}"
            )

    def __init__(
        self,
        keys: list[str],
        chunking: Chunking | Chunking.FixedSizeChunk,
        name: str | None = None,
    ):
        """Defines which MARS keys form an axis in the zarr array, and how it is chunked.

        Args:
            keys (list[str]): MARS keys that form this axis.
            chunking (~pychunked_data_view.Chunking | ~pychunked_data_view.Chunking.FixedSizeChunk):
                How this axis shall be chunked.
            name (str | None): Zarr dimension name. Defaults to the keys joined by ``"_"``.
        """
        self._obj = pdv.AxisDefinition(keys=keys, chunking=self._translate_chunking(chunking), name=name)

    @property
    def name(self) -> str | None:
        """The zarr dimension name for this axis, or None to derive it from the keys."""
        return self._obj.name

    @name.setter
    def name(self, name: str | None) -> None:
        self._obj.name = name

    @property
    def keys(self) -> list[str]:
        """The MARS keys that form this axis."""
        return self._obj.keys

    @keys.setter
    def keys(self, keys: list[str]) -> None:
        self._obj.keys = keys

    @property
    def chunking(self) -> Chunking | Chunking.FixedSizeChunk:
        """The chunking strategy for this axis.

        Raises:
            ~pychunked_data_view.exceptions.InternalError: If the underlying C++ chunking
                type is unrecognised.
        """
        chunking = self._obj.chunking
        if isinstance(chunking, pdv.AxisDefinition.WholeAxisChunking):
            return Chunking.WHOLE_AXIS
        elif isinstance(chunking, pdv.AxisDefinition.SingleValueChunking):
            return Chunking.SINGLE_VALUE
        elif isinstance(chunking, pdv.AxisDefinition.FixedSizeChunking):
            return Chunking.FixedSizeChunk(chunking.chunk_shape)
        else:
            raise InternalError()

    @chunking.setter
    def chunking(self, chunking: Chunking | Chunking.FixedSizeChunk) -> None:
        self._obj.chunking = self._translate_chunking(chunking)


class ChunkedDataView:
    """Python wrapper around the C++ ``ChunkedDataView``.

    Provides shape and chunk-count metadata, and per-chunk data access via
    :meth:`at`.  Instances are returned by :meth:`ChunkedDataViewBuilder.build`.
    """

    def __init__(self, obj: pdv.ChunkedDataView):
        self._obj = obj

    def at(self, index: list[int] | tuple[int, ...]) -> "numpy.ndarray":
        """Return the values of the chunk at *index*.

        Args:
            index (list[int] | tuple[int, ...]): Per-dimension chunk coordinates, including
                the implicit grid-point dimension.

        Returns:
            numpy.ndarray: 1-D ``float32`` array of ``chunk_shape()`` values, C-order.

        Raises:
            RuntimeError: If *index* is out of bounds or the FDB retrieval fails.
        """
        return self._obj.at(index)

    def chunk_shape(self) -> tuple[int, ...]:
        """Return the per-dimension element count of one chunk.

        Returns:
            tuple[int, ...]: Number of elements along each dimension within a single chunk.
        """
        return self._obj.chunk_shape()

    def chunkShape(self) -> tuple[int, ...]:
        """Deprecated alias of :meth:`chunk_shape`.

        Kept so existing callers keep working; every other method on this class is
        snake_case.
        """
        warnings.warn(
            "ChunkedDataView.chunkShape() is deprecated, use chunk_shape() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.chunk_shape()

    def chunks(self) -> tuple[int, ...]:
        """Return the per-dimension number of chunks.

        Returns:
            tuple[int, ...]: Number of chunks along each dimension.
        """
        return self._obj.chunks()

    def shape(self) -> tuple[int, ...]:
        """Return the total array shape in elements (not chunks).

        Returns:
            tuple[int, ...]: Total number of elements along each dimension.
        """
        return self._obj.shape()

    def fill_missing_value(self) -> float:
        """Return the fill value used for bitmap-masked grid points.

        Returns:
            float: Value written into positions flagged as missing by the GRIB bitmap.
        """
        return self._obj.fill_missing_value()


class ExtractorType:
    """Namespace for extractor configuration types.

    * :class:`ExtractorType.Grib`     - standard full-field GRIB extraction.
    * :class:`ExtractorType.GribJump` - partial-field extraction via GribJump.

    Each class wraps the matching C++ ``ExtractorDefinition``, which is what
    :meth:`ChunkedDataViewBuilder.add_part` takes.

    One instance may be reused across as many parts and builders as you like:
    ``add_part`` stores a copy, so defaults the builder applies (e.g. its
    ``fdb_config``) are never written back into your object.
    """

    class Grib:
        """Reads full GRIB fields from FDB.

        Args:
            fdb_config (pathlib.Path | None): Path to the FDB configuration YAML.
                ``None`` (default) uses the builder's FDB config.
        """

        def __init__(self, fdb_config: pathlib.Path | None = None):
            self._obj = pdv.ExtractorType.Grib(fdb_config=fdb_config)

    class GribJump:
        """Reads grid-point values from FDB via GribJump.

        GribJump avoids a full GRIB decode by jumping directly to the
        grid-point values inside each message.

        Args:
            fdb_config      (pathlib.Path | None): Path to the FDB configuration YAML.
                ``None`` (default) uses the builder's FDB config.
            gribjump_config (pathlib.Path | None): Path to the GribJump configuration YAML.
                ``None`` (default) reads the ``GRIBJUMP_CONFIG_FILE`` environment variable.
            field_chunking  (~pychunked_data_view.Chunking | ~pychunked_data_view.Chunking.FixedSizeChunk | None):
                How to sub-divide the implicit (grid-point) dimension into Zarr chunks.
                ``None`` (default) produces a single chunk covering the full field. The size
                must divide the grid exactly -- that dimension cannot be left ragged.
        """

        def __init__(
            self,
            fdb_config: pathlib.Path | None = None,
            gribjump_config: pathlib.Path | None = None,
            field_chunking: "Chunking | Chunking.FixedSizeChunk | None" = None,
        ):
            self._obj = pdv.ExtractorType.GribJump(
                fdb_config=fdb_config,
                gribjump_config=gribjump_config,
                field_chunking=(
                    AxisDefinition._translate_chunking(field_chunking) if field_chunking is not None else None
                ),
            )


class ChunkedDataViewBuilder:
    """Collects MARS request parts and builds a :class:`ChunkedDataView`.

    Wraps the C++ ``ChunkedDataViewBuilder``.  Call :meth:`add_part` one or
    more times, then :meth:`build` to obtain the view.

    Args:
        fdb_config_file (pathlib.Path | None): Path to the FDB configuration YAML.
            ``None`` lets FDB resolve its configuration from the environment.
    """

    def __init__(self, fdb_config_file: pathlib.Path | None):
        self._obj = pdv.ChunkedDataViewBuilder(fdb_config_file)
        self._dim_names: list[str] | None = None

    def add_part(
        self,
        mars_request: MarsSelection,
        axes: list[AxisDefinition],
        extractor: ExtractorType.Grib | ExtractorType.GribJump,
    ) -> "ChunkedDataViewBuilder":
        """Validate *axes* against *mars_request*, record dimension names, and register the part.

        Args:
            mars_request (MarsSelection): MARS key-value mapping describing the data to retrieve.
            axes (list[AxisDefinition]): Axis definitions; each must reference keys present in
                *mars_request*.
            extractor (ExtractorType.Grib | ExtractorType.GribJump): Extraction backend to use.

        Returns:
            ChunkedDataViewBuilder: ``self``, for method chaining.

        Raises:
            ValueError: If any axis key is not present in *mars_request*.

        Note:
            Only the axis-key check happens here. Everything that needs FDB -- field sizes,
            axis mapping, whether the parts fit together -- is validated by :meth:`build`,
            which raises ``RuntimeError`` on any of it.
        """
        for ax in axes:
            missing = [k for k in ax.keys if k not in mars_request]
            if missing:
                raise ValueError(
                    f"ChunkedDataViewBuilder::add_part: Axis key(s) {missing!r} not found in the MARS request. "
                    f"Available keys: {sorted(mars_request.keys())!r}. "
                    f"Check for typos in the AxisDefinition."
                )
        if self._dim_names is None:
            self._dim_names = [ax.name if ax.name is not None else "_".join(ax.keys) for ax in axes]
            self._dim_names.append("values")  # implicit grid-point axis
        self._obj.add_part(
            _mars_selection_to_string(mars_request),
            [ax._obj for ax in axes],
            extractor._obj,
        )
        return self

    def dim_names(self) -> list[str]:
        """Return the zarr dimension names derived from the first registered part.

        Returns:
            list[str]: One name per axis (MARS keys joined by ``_``), plus ``"values"``
                for the implicit grid-point axis. Empty if no part has been added yet.
        """
        return self._dim_names or []

    def extend_on_axis(self, axis: int) -> "ChunkedDataViewBuilder":
        """Set *axis* as the extension axis when multiple parts are added.

        Args:
            axis (int): Zero-based index of the axis along which parts are concatenated.

        Returns:
            ChunkedDataViewBuilder: ``self``, for method chaining.
        """
        self._obj.extend_on_axis(axis)
        return self

    def fill_missing_value(self, value: float) -> "ChunkedDataViewBuilder":
        """Set the fill value for bitmap-masked grid points.

        Args:
            value (float): Value written into positions flagged as missing by the GRIB bitmap.
                Also used as the zarr array ``fill_value``.

        Returns:
            ChunkedDataViewBuilder: ``self``, for method chaining.
        """
        self._obj.fill_missing_value(value)
        return self

    def build(self) -> ChunkedDataView:
        """Build and return the :class:`ChunkedDataView`.

        Returns:
            ChunkedDataView: The assembled view, ready for chunk-level data access.

        Raises:
            ~pychunked_data_view.exceptions.MarsRequestFormattingError: If the MARS request
                string is malformed
                (trailing comma, missing comma between keys, or misspelled key).
        """
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
