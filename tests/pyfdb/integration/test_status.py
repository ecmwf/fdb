# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import pytest

from pyfdb.pyfdb import FDB
from pyfdb.pyfdb_iterator import StatusElement

# Instants well before anything these tests archive.
READ_TIME = 1_000_000_000
WRITE_TIME = 1_000_000_500

DATABASE = {
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "date": "20200101",
    "time": "1800",
}


def status_of_database(fdb: FDB) -> StatusElement:
    elements = list(fdb.status(DATABASE))
    assert len(elements) == 1
    return elements[0]


@pytest.fixture
def database(read_write_fdb_setup) -> tuple[FDB, Path]:
    """An FDB, and the directory of the database selected by `DATABASE`"""
    fdb = FDB(read_write_fdb_setup)
    return fdb, Path(status_of_database(fdb).location().path())


def test_status(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
    }

    status_iterator = fdb.status(selection)

    elements = []

    for el in status_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 32

    expected_key = {
        "class": ["ea"],
        "date": ["20200104"],
        "domain": ["g"],
        "expver": ["0001"],
        "stream": ["oper"],
    }
    expected_control_identifiers: list[StatusElement] = []

    for element in elements:
        assert all(
            el in element.key() for el in expected_key
        )  # Check whether the expected key is part of the elements

        assert element.controlIdentifiers() == expected_control_identifiers

        date = element.key()["date"]
        assert date and len(date) == 1
        time = element.key()["time"]
        assert time and len(time) == 1

        assert "ea:0001:oper:" in str(element.location())
        assert f":{date[0]}:" in str(element.location())
        assert f":{time[0]}:" in str(element.location())
        assert ":g" in str(element.location())

    # Test compare of status elements
    assert elements[0] == elements[0]
    assert elements[1] != elements[0]


def test_status_access_times_of_new_database(read_write_fdb_setup):
    fdb = FDB(read_write_fdb_setup)
    element = status_of_database(fdb)

    # Archiving creates both records, and each is a plain integer of epoch seconds.
    assert isinstance(element.last_read(), int)
    assert isinstance(element.last_write(), int)
    assert element.last_read() > WRITE_TIME
    assert element.last_write() > WRITE_TIME


def test_status_access_times_determinate(database):
    fdb, db = database

    os.utime(db / "read.timestamp", (READ_TIME, READ_TIME))
    os.utime(db / "write.timestamp", (WRITE_TIME, WRITE_TIME))

    element = status_of_database(fdb)
    assert element.last_read() == READ_TIME
    assert element.last_write() == WRITE_TIME
    assert f"last_read={READ_TIME}" in repr(element)
    assert f"last_write={WRITE_TIME}" in repr(element)

    # Querying the status is not an access, and leaves them alone.
    element = status_of_database(fdb)
    assert element.last_read() == READ_TIME
    assert element.last_write() == WRITE_TIME
    assert int((db / "read.timestamp").stat().st_mtime) == READ_TIME
    assert int((db / "write.timestamp").stat().st_mtime) == WRITE_TIME


@pytest.mark.parametrize(
    "missing, expected_read, expected_write",
    [
        pytest.param(["read.timestamp"], None, WRITE_TIME, id="read"),
        pytest.param(["write.timestamp"], READ_TIME, None, id="write"),
        pytest.param(["read.timestamp", "write.timestamp"], None, None, id="both"),
    ],
)
def test_status_access_times_unknown(database, missing, expected_read, expected_write):
    fdb, db = database

    os.utime(db / "read.timestamp", (READ_TIME, READ_TIME))
    os.utime(db / "write.timestamp", (WRITE_TIME, WRITE_TIME))
    for name in missing:
        (db / name).unlink()

    element = status_of_database(fdb)
    assert element.last_read() == expected_read
    assert element.last_write() == expected_write
    assert f"last_read={expected_read}," in repr(element)
    assert f"last_write={expected_write})" in repr(element)

    # Querying the status does not create a missing record.
    assert status_of_database(fdb).last_read() == expected_read
    for name in missing:
        assert not (db / name).exists()


def test_status_access_times_do_not_affect_equality(database):
    fdb, db = database

    os.utime(db / "read.timestamp", (READ_TIME, READ_TIME))
    before = status_of_database(fdb)

    (db / "read.timestamp").unlink()
    after = status_of_database(fdb)

    assert before.last_read() != after.last_read()
    assert before == after
