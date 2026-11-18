# SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import pytest

from pyfdb import FDB, AccessIdentifier, AccessTime, StatusElement
from pyfdb._internal import _AccessIdentifier

# An instant well before anything these tests archive.
LONG_AGO = 1_000_000_000

DATABASE = {
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "date": "20200101",
    "time": "1800",
}

BOTH = [AccessIdentifier.READ, AccessIdentifier.WRITE]


def database_dir(fdb: FDB) -> Path:
    elements = list(fdb.status(DATABASE))
    assert len(elements) == 1
    return Path(elements[0].location().path())


def mtime(path: Path) -> int:
    return int(path.stat().st_mtime)


@pytest.fixture
def fdb(read_write_fdb_setup) -> FDB:
    return FDB(read_write_fdb_setup)


@pytest.fixture
def read_timestamp(fdb) -> Path:
    return database_dir(fdb) / "read.timestamp"


@pytest.fixture
def write_timestamp(fdb) -> Path:
    return database_dir(fdb) / "write.timestamp"


def test_enum_access_identifiers():
    assert AccessIdentifier.NONE == 0
    assert AccessIdentifier.READ == 1
    assert AccessIdentifier.WRITE == 2


@pytest.mark.parametrize(
    "variant, expected",
    [
        (_AccessIdentifier.NONE, AccessIdentifier.NONE),
        (_AccessIdentifier.READ, AccessIdentifier.READ),
        (_AccessIdentifier.WRITE, AccessIdentifier.WRITE),
    ],
)
def test_enum_access_identifiers_mapping(variant, expected):
    assert variant.value == expected.value
    assert AccessIdentifier._from_raw(variant) == expected
    assert expected._to_raw() == variant


def test_access_identifier_string():
    assert str(AccessIdentifier.NONE) == "NONE"
    assert str(AccessIdentifier.READ) == "READ"
    assert str(AccessIdentifier.WRITE) == "WRITE"
    assert str(AccessIdentifier.READ | AccessIdentifier.WRITE) == "READ|WRITE"


def test_access_time_repr():
    assert repr(AccessTime.now()) == "AccessTime[now]"
    assert repr(AccessTime.catalogue()) == "AccessTime[catalogue]"
    assert repr(AccessTime.at(LONG_AGO)) == f"AccessTime[{LONG_AGO}]"


def test_access_time_not_constructible_from_user_code():
    with pytest.raises(TypeError):
        AccessTime(AccessTime.now()._to_raw())


def test_touch_records_explicit_instant(fdb, read_timestamp, write_timestamp):
    elements = list(fdb.touch(DATABASE, BOTH, AccessTime.at(LONG_AGO)))

    assert len(elements) == 1
    assert isinstance(elements[0], StatusElement)
    assert elements[0].key()["date"] == ["20200101"]
    assert elements[0].last_read() == LONG_AGO
    assert elements[0].last_write() == LONG_AGO
    assert mtime(read_timestamp) == LONG_AGO
    assert mtime(write_timestamp) == LONG_AGO

    # Each identifier selects only its own timestamp.
    list(fdb.touch(DATABASE, [AccessIdentifier.READ], AccessTime.at(LONG_AGO + 1)))
    assert mtime(read_timestamp) == LONG_AGO + 1
    assert mtime(write_timestamp) == LONG_AGO

    list(fdb.touch(DATABASE, [AccessIdentifier.WRITE], AccessTime.at(LONG_AGO + 2)))
    assert mtime(read_timestamp) == LONG_AGO + 1
    assert mtime(write_timestamp) == LONG_AGO + 2


def test_touch_defaults_to_now(fdb, read_timestamp, write_timestamp):
    list(fdb.touch(DATABASE, BOTH, AccessTime.at(LONG_AGO)))

    list(fdb.touch(DATABASE, [AccessIdentifier.READ]))

    assert mtime(read_timestamp) > LONG_AGO
    assert mtime(write_timestamp) == LONG_AGO


def test_touch_from_catalogue(fdb, read_timestamp, write_timestamp):
    list(fdb.touch(DATABASE, BOTH, AccessTime.at(LONG_AGO)))

    toc_time = LONG_AGO + 500
    os.utime(read_timestamp.parent / "toc", (toc_time, toc_time))

    list(fdb.touch(DATABASE, [AccessIdentifier.READ], AccessTime.catalogue()))

    assert mtime(read_timestamp) == toc_time
    assert mtime(write_timestamp) == LONG_AGO


def test_touch_creates_missing_record(fdb, read_timestamp):
    read_timestamp.unlink()

    list(fdb.touch(DATABASE, [AccessIdentifier.READ], AccessTime.at(LONG_AGO)))

    assert mtime(read_timestamp) == LONG_AGO


def test_touch_records_nothing_until_consumed(fdb, read_timestamp):
    list(fdb.touch(DATABASE, BOTH, AccessTime.at(LONG_AGO)))

    fdb.touch(DATABASE, [AccessIdentifier.READ], AccessTime.at(LONG_AGO + 1))

    assert mtime(read_timestamp) == LONG_AGO


def test_touch_no_identifiers_records_nothing(fdb, read_timestamp, write_timestamp):
    list(fdb.touch(DATABASE, BOTH, AccessTime.at(LONG_AGO)))

    assert len(list(fdb.touch(DATABASE, []))) == 1

    assert mtime(read_timestamp) == LONG_AGO
    assert mtime(write_timestamp) == LONG_AGO


@pytest.mark.parametrize(
    "selection",
    [
        pytest.param({**DATABASE, "date": "20200105"}, id="no matching database"),
        pytest.param({**DATABASE, "type": "an"}, id="key beyond the database level"),
    ],
)
def test_touch_selection_matches_nothing(fdb, read_timestamp, selection):
    list(fdb.touch(DATABASE, BOTH, AccessTime.at(LONG_AGO)))

    assert list(fdb.touch(selection, [AccessIdentifier.READ])) == []

    assert mtime(read_timestamp) == LONG_AGO


def test_touch_partial_and_wildcard_selection(fdb):
    partial = {"class": "ea", "date": "20200101"}
    assert len(list(fdb.touch(partial, [AccessIdentifier.READ], AccessTime.at(LONG_AGO)))) == 8

    assert len(list(fdb.touch({}, [AccessIdentifier.READ], AccessTime.at(LONG_AGO)))) == 32


def test_touch_failure_raises_during_iteration(fdb, read_timestamp):
    database = read_timestamp.parent
    original_mode = database.stat().st_mode

    # A missing record cannot be recreated in a read-only database.
    read_timestamp.unlink()
    database.chmod(0o555)

    try:
        if os.access(database, os.W_OK):
            pytest.skip("Permissions don't constrain this user, e.g. root")

        touch_iterator = fdb.touch(DATABASE, [AccessIdentifier.READ])
        with pytest.raises(RuntimeError, match="Updating timestamp failed"):
            list(touch_iterator)
    finally:
        database.chmod(original_mode)
