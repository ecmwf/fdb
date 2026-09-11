# SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""
z3FDB enables to create views into FDB where the view is a Zarr array.

Views are defined by one or more MARS requests. Each keyword in the MARS
request with more than one value defines an 'Axis'. 'Axis' from MARS requests
need to be mapped to 'Axis' in the Zarr array. This mapping can be a 1-1 or
many-1 mapping, allowing to create a time based axis in the Zarr array that
is composed from the 'date' and 'time' keyword when dealing with climate data.

For example, the request
``"..., date=1970-01-1/to/2020-12-31, time=00/06/12/18, ..."`` spans two axis
'date' and 'time'. If you want to work on a unified time axis, then you  can
use the following :class:`AxisDefinition` to map accordingly:

Example::

    AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)

This defines an 'Axis' in the Zarr array that follows 'date' and 'time' from
the MARS request, where the rightmost references 'Axis' ('time') is varying
fastest.

You can combine multiple MARS request into one view. This is useful if you
want to access surface and pressure level data in one view. In this case you
need to select on which 'Axis' of the Zarr array the requests extend each
other. The remaining axis have to have the same cardinality.

.. code-block:: python

   builder.add_part(
       {
           "type": "an",
           "class": "ea",
           "domain": "g",
           "expver": "0001",
           "stream": "oper",
           "date": ["2020-01-01", "2020-01-02"],
           "levtype": "sfc",
           "step": 0,
           "param": [165, 166],
           "time": "0/to/21/by/3",
       },
       [
           AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
           AxisDefinition(["param"], Chunking.SINGLE_VALUE)
       ],
       ExtractorType.Grib(),
   )
   builder.add_part(
       {
           "type": "an",
           "class": "ea",
           "domain": "g",
           "expver": "0001",
           "stream": "oper",
           "date": ["2020-01-01", "2020-01-02"],
           "levtype": "pl",
           "step": 0,
           "param": [131, 132],
           "levelist": [50, 100],
           "time": "0/to/21/by/3",
       },
       [
           AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
           AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE)
       ],
       ExtractorType.Grib(),
   )
   builder.extend_on_axis(1)
   store = builder.build()

The created Zarr array will always have the actual data points available as
the final 'Axis'.

.. code-block:: text

    arr[0][0][0][0]
        ^  ^  ^  ^
        |  |  | Index in field  -> Implicit
        |  | Ensemble           -> Created from an AxisDefinition
        | Step                  -> Created from an AxisDefinition
       Date                     -> Created from an AxisDefinition

"""

from pychunked_data_view.chunked_data_view import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    MarsSelection,
)
from z3fdb.simple_store_builder import SimpleStoreBuilder
from z3fdb.custom_store_builder import CustomStoreBuilder
from z3fdb.z3fdb_error import Z3fdbError

__all__ = [
    "AxisDefinition",
    "Chunking",
    "CustomStoreBuilder",
    "ExtractorType",
    "MarsSelection",
    "SimpleStoreBuilder",
    "Z3fdbError",
]
