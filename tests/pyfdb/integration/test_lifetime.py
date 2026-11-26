# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""
Tests for object-lifetime safety in pyfdb.

A DataHandle returned by FDB.retrieve() must remain usable after the FDB
object that created it has been destroyed. The C++ FDB object manages the
connection and catalogue state; if the DataHandle does not keep it alive the
read() call below will access freed memory or hit a null pointer and crash.
"""

import gc

from pyfdb import FDB

_SELECTION = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "levtype": "sfc",
    "step": "0",
    "time": "1800",
    "param": ["167", "131", "132"],
    "date": "20200101",
}


def get_datahandle(read_only_fdb_setup):

    with FDB(read_only_fdb_setup) as fdb:
        dh = fdb.retrieve(_SELECTION)
        dh.open()

        first_bytes = dh.read(4)
        assert first_bytes == b"GRIB", (
            f"Unexpected magic bytes before FDB deletion: {first_bytes!r}"
        )

        # Drop the only Python reference to the FDB object and force collection so
        # the C++ destructor fires now rather than at an indeterminate later time.
        del fdb
        gc.collect()

        return dh


def test_datahandle_outlives_fdb(read_only_fdb_setup):
    """
    A DataHandle must remain readable after the FDB object that created it has
    been explicitly destroyed.

    Steps:
    1. Create an FDB instance and retrieve a DataHandle.
    2. Open the handle and read the first 4 bytes to confirm it is live.
    3. Explicitly delete the FDB object and force a garbage-collection cycle so
       that the C++ destructor runs before the next read.
    4. Read further from the handle — if the DataHandle does not extend the
       lifetime of the underlying FDB internals this will crash or raise.
    """
    dh = get_datahandle(read_only_fdb_setup)

    # The DataHandle must still be readable after the FDB object is gone.
    further_bytes = dh.read(4)
    assert len(further_bytes) == 4, f"Expected 4 bytes after FDB deletion, got {len(further_bytes)}"
    dh.close()
