# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import pytest
from eccodes import StreamReader

import support.util as util
from pyfdb.pyfdb import FDB, Key, Request

STATIC_DICTIONARY = {
    "class": "rd",
    "date": "20191110",
    "domain": "g",
    "expver": "xxxx",
    "levelist": "300",
    "levtype": "pl",
    "param": "138",
    "step": "0",
    "stream": "oper",
    "time": "0000",
    "type": "an",
}


def assert_one_field(fdb: FDB):
    dataretriever = fdb.retrieve(STATIC_DICTIONARY)
    reader = StreamReader(dataretriever)
    _ = next(reader)

    list_iterator = fdb.list(STATIC_DICTIONARY, duplicates=True, keys=True)
    assert len([x for x in list_iterator]) == 1

    with pytest.raises(StopIteration) as _:
        next(reader)


def test_archive_none(setup_fdb_tmp_dir, data_path):
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read())
    fdb.flush()

    assert_one_field(fdb)


def test_archive_no_request(setup_fdb_tmp_dir, data_path):
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), None)
    fdb.flush()

    assert_one_field(fdb)


def test_archive_no_request_no_key(setup_fdb_tmp_dir, data_path):
    """This results in the resolution of request data from the corresponding fdb call"""
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), None, None)
    fdb.flush()

    assert_one_field(fdb)


def test_archive_key(setup_fdb_tmp_dir, data_path):
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    key = Key(STATIC_DICTIONARY)

    fdb.archive(open(filename, "rb").read(), key=key)
    fdb.flush()


def test_archive_request(setup_fdb_tmp_dir, data_path):
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    request = Request(STATIC_DICTIONARY)

    fdb.archive(open(filename, "rb").read(), request=request)
    fdb.flush()


def test_archive_request_dict(setup_fdb_tmp_dir, data_path):
    """Tests whether archival with dict and request leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), request=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), request=Request(STATIC_DICTIONARY))
    fdb.flush()

    assert_one_field(fdb)


def test_archive_key_dict(setup_fdb_tmp_dir, data_path):
    """Tests whether archival with dict and key leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), key=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), key=Key(STATIC_DICTIONARY))
    fdb.flush()

    assert_one_field(fdb)


def test_archive_request_dict_key(setup_fdb_tmp_dir, data_path):
    """Tests whether archival with request dict and key leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), request=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), key=Key(STATIC_DICTIONARY))
    fdb.flush()

    assert_one_field(fdb)


def test_archive_request_key_dict(setup_fdb_tmp_dir, data_path):
    """Tests whether archival with request and key dict leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), key=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), request=Request(STATIC_DICTIONARY))
    fdb.flush()

    assert_one_field(fdb)


def test_archive_request_dict_key_dict(setup_fdb_tmp_dir, data_path):
    """Tests whether archival with request and key dict leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), key=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), request=STATIC_DICTIONARY)
    fdb.flush()

    assert_one_field(fdb)
