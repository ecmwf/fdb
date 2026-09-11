# SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from z3fdb._internal.zarr import FdbZarrStore
from z3fdb.custom_store_builder import CustomStoreBuilder
from pychunked_data_view import (
    AxisDefinition,
    ExtractorType,
    MarsSelection,
)


class SimpleStoreBuilder:
    """Builder to create a Zarr store with FDB backing.

    This builder will create a Zarr store with a Zarr Array at its root ("/")
    containing the data from your MARS request(s).

    It is exactly :class:`~z3fdb.CustomStoreBuilder` restricted to the root array, and
    delegates to it -- so the two cannot drift apart.

    Args:
        fdb_config_file: Optional path to FDB config file. If not set normal
            FDB config file resolution is applied.
    """

    def __init__(self, fdb_config_file: Path | None = None):
        self._builder = CustomStoreBuilder(fdb_config_file)

    def add_part(
        self,
        mars_request: MarsSelection,
        axes: list[AxisDefinition],
        extractor: ExtractorType.Grib | ExtractorType.GribJump,
    ) -> None:
        """Add a MARS request to the view.

        Args:
            mars_request(MarsSelection):
                A dict mapping MARS keys to their values. Single values may be
                given as ``str``, ``int``, or ``float``; multi-valued keys may
                be given as a list. MARS range expressions (e.g.
                ``"2020-01-01/to/2020-01-04"``) must be passed as a plain
                string value.

                For example::

                    {
                        "type": "an",
                        "class": "ea",
                        "domain": "g",
                        "expver": "0001",
                        "stream": "oper",
                        "date": "2020-01-01/to/2020-01-04",
                        "levtype": "sfc",
                        "step": 0,
                        "param": [167, 131, 132],
                        "time": "0/to/21/by/3",
                    }

            axes(:obj:`list` of :obj:`AxisDefinition`):  List of
                AxisDefinitions that describe how axis in the MARS request are
                mapped to axis in the Zarr array.
            extractor: Extractor configuration object. Use
                ``ExtractorType.Grib()`` for full-field GRIB extraction or
                ``ExtractorType.GribJump(...)`` for partial-field extraction.
        """
        self._builder.add_part(None, mars_request, axes, extractor)

    def fill_missing_value(self, value: float) -> None:
        """Set the fill value used for bitmap-masked grid points.

        Call :meth:`add_part` first: this configures the array, so there has to be one.

        Args:
            value(float): Fill value written into array positions that carry a
                GRIB bitmap missing flag. Also used as the zarr array fill_value.

        Raises:
            ValueError: If no part has been added yet.
        """
        self._builder.fill_missing_value(None, value)

    def extend_on_axis(self, axis: int) -> None:
        """Defines the extension axis when multiple parts are added.

        Call :meth:`add_part` first: this configures the array, so there has to be one.

        Args:
            axis(int): Index of the axis that is extended when multiple parts
                have been added.

        Raises:
            ValueError: If no part has been added yet.
        """
        self._builder.extend_on_axis(None, axis)

    def build(self) -> FdbZarrStore:
        """Build the store from the registered parts.

        Returns:
            :class:`~z3fdb._internal.zarr.FdbZarrStore` ready to pass to
            ``zarr.open_array()``.

        Raises:
            RuntimeError: If the view is misconfigured -- no parts, a missing extension axis,
                incompatible part shapes, or parts that disagree about the grid. These
                originate as ``eckit::UserError`` in the C++ layer.
        """
        return self._builder.build()
