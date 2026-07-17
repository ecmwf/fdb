# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from zarr.abc.store import Store
from pathlib import Path

from z3fdb._internal.zarr import FdbZarrStore, FdbZarrArray, FdbSource
from pychunked_data_view import (
    ChunkedDataViewBuilder,
    AxisDefinition,
    ExtractorType,
    MarsSelection,
)


class SimpleStoreBuilder:
    """Builder to create a Zarr store with FDB backing.

    This builder will create a Zarr store with a Zarr Array at its root ("/")
    containing the data from your MARS request(s).

    Args:
        fdb_config_file: Optional path to FDB config file. If not set normal
            FDB config file resolution is applied.
    """

    def __init__(self, fdb_config_file: Path | None = None):
        self._builder = ChunkedDataViewBuilder(fdb_config_file)

    def add_part(
        self,
        mars_request: MarsSelection,
        axes: list[AxisDefinition],
        extractor_type: ExtractorType,
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
            extractor_type: Defines how to extract data from FDB. Currently
                only ExtractorType.GRIB is supported.
        """
        self._builder.add_part(mars_request, axes, extractor_type)

    def fill_value(self, value: float) -> None:
        """Set the fill value used for missing / bitmap-masked grid points.

        Args:
            value(float): Fill value written into array positions that carry a
                GRIB bitmap missing flag. Also used as the zarr array fill_value.
        """
        self._builder.fill_value(value)

    def extend_on_axis(self, axis: int) -> None:
        """Defines the extension axis when multiple parts are added.

        Args:
            axis(int): Index of the axis that is extendet when multiple parts
                have been added.

        """
        self._builder.extend_on_axis(axis)

    def build(self) -> Store:
        """Build the store from the inputs.

        Raises:
            Z3fdbError if store cannot be created.

        """
        return FdbZarrStore(
            FdbZarrArray(
                datasource=FdbSource(self._builder.build()),
            )
        )
