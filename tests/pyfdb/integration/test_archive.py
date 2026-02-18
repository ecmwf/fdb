from pathlib import Path
import pytest
from pyfdb import FDB
from pyfdb.pyfdb_type import Identifier

import psutil
import os


def assert_one_field(pyfdb: FDB):
    selection = {
        "class": "rd",
        "expver": "xxxx",
        "stream": "oper",
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": "0",
        "levelist": "300",
        "param": "138",
    }

    with pyfdb.retrieve(selection) as data_handle:
        assert data_handle

    list_iterator = pyfdb.list(selection)

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1

    with pytest.raises(StopIteration) as _:
        next(list_iterator)


def test_archive_none(empty_fdb_setup, test_data_path):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)
    filename: Path = test_data_path / "x138-300.grib"

    fdb.archive(filename.read_bytes())
    fdb.flush()

    assert_one_field(fdb)


def print_open_files_psutil(pid=None):
    """
    Prints open files for a process (default: current).
    Requires psutil. On some OSes, elevated privileges may be required
    to inspect other processes.
    """
    if pid is None:
        pid = os.getpid()
    proc = psutil.Process(pid)
    try:
        files = proc.open_files()
    except psutil.AccessDenied:
        print(f"Access denied for pid {pid}. Try running with higher privileges.")
        return
    except psutil.NoSuchProcess:
        print(f"No such process: {pid}")
        return

    if not files:
        print(f"No open files for pid {pid}.")
        return

    print(f"Open files for pid {pid}:")
    for f in files:
        print(f" - path={f.path}  |  fd={getattr(f, 'fd', '?')}")


def test_archive_abitrary_data(empty_fdb_setup):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    selection = {
        "class": "rd",
        "expver": "arb0",
        "stream": "oper",
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": "0",
        "levelist": "300",
        "param": "138",
    }

    print_open_files_psutil()

    print("-----------ARCHIVE-------------------")

    with FDB(fdb_config_path) as fdb:
        fdb.archive(
            data=b"binary-data",
            identifier=Identifier(selection),
        )

    print_open_files_psutil()

    print("-----------RETRIEVE-------------------")

    data_handle = fdb.retrieve(selection)
    file_content = data_handle.readall()
    assert file_content == b"binary-data"

    print_open_files_psutil()

    print("-----------CLOSE-------------------")

    data_handle.close()

    print_open_files_psutil()


key_values = [
    ("stream", "oper/rd"),
    ("stream", ["oper", "rd"]),
]


@pytest.mark.parametrize("wrong_key_value", key_values)
def test_archive_abitrary_data_wrong_identifier(empty_fdb_setup, wrong_key_value):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    selection = {
        "class": "rd",
        "expver": "arb1",
        wrong_key_value[0]: wrong_key_value[1],
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": "0",
        "levelist": "300",
        "param": "138",
    }

    with pytest.raises(ValueError):
        with FDB(fdb_config_path) as fdb:
            fdb.archive(
                data=b"binary-data",
                identifier=Identifier(selection),
            )


def test_archive_round_trip(empty_fdb_setup, test_data_path):
    fdb_config_path = empty_fdb_setup
    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    filename = test_data_path / "x138-300.grib"
    file_content = filename.read_bytes()

    print("-----------ARCHIVE-------------------")
    print(f"Read {len(file_content)} bytes")
    fdb.archive(file_content)
    fdb.flush()

    print_open_files_psutil()

    print("-----------RETRIEVE-------------------")

    # Those are the FDB keys of the GRIB file given above
    selection = {
        "class": "rd",
        "expver": "xxxx",
        "stream": "oper",
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": "0",
        "levelist": "300",
        "param": "138",
    }

    with fdb.retrieve(selection) as data_handle:
        reread_file_content = data_handle.readall()
        print(f"Original size: {len(file_content)}")
        print(f"Reread size: {len(reread_file_content)}")
        assert file_content == reread_file_content

    list_iterator = fdb.list({})

    print_open_files_psutil()

    assert len(list(list_iterator)) == 1
