from pyfdb import Config
from pyfdb import PyFDB
from pyfdb import FDBToolRequest
from pyfdb.pyfdb_type import MarsRequest
from pyfdb_bindings.pyfdb_bindings import ControlAction, ControlIdentifier

import pytest


def test_control_action_values():
    assert ControlAction.NONE == 0
    assert ControlAction.DISABLE == 1
    assert ControlAction.ENABLE == 2


def test_control_identifier_values():
    assert ControlIdentifier.NONE == 0
    assert ControlIdentifier.LIST == 1
    assert ControlIdentifier.RETRIEVE == 2
    assert ControlIdentifier.ARCHIVE == 4
    assert ControlIdentifier.WIPE == 8
    assert ControlIdentifier.UNIQUEROOT == 16


def test_control_lock_retrieve(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        request = FDBToolRequest(
            {
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": "20200101",
                "time": "1800",
            },
        )

        print("Retrieve without lock")
        data_handle = pyfdb.retrieve(
            MarsRequest(
                "retrieve",
                {
                    "type": "an",
                    "class": "ea",
                    "domain": "g",
                    "expver": "0001",
                    "stream": "oper",
                    "date": "20200101",
                    "levtype": "sfc",
                    "step": "0",
                    "param": "167/165/166",
                    "time": "1800",
                },
            )
        )
        assert data_handle
        assert data_handle.read(4) == b"GRIB"

        print("Locking database for retrieve")

        control_iterator = pyfdb.control(
            request, ControlAction.DISABLE, [ControlIdentifier.RETRIEVE]
        )
        assert control_iterator

        elements = []

        for el in control_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1

        print("Retrieve with lock")
        with pytest.raises(Exception) as er:
            data_handle = pyfdb.retrieve(
                MarsRequest(
                    "retrieve",
                    {
                        "type": "an",
                        "class": "ea",
                        "domain": "g",
                        "expver": "0001",
                        "stream": "oper",
                        "date": "20200101",
                        "levtype": "sfc",
                        "step": "0",
                        "param": "167/165/166",
                        "time": "1800",
                    },
                )
            )


def test_control_lock_list(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        request = FDBToolRequest(
            {
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": "20200101",
                "time": "1800",
            },
        )

        print("List without lock")
        list_iterator = pyfdb.list(request)
        elements = list(list_iterator)
        assert len(elements) == 3

        print("Locking database for listing")
        control_iterator = pyfdb.control(
            request, ControlAction.DISABLE, [ControlIdentifier.LIST]
        )
        assert control_iterator

        elements = []

        for el in control_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1

        print("List with lock. Expecting 0 elements.")
        list_iterator = pyfdb.list(request)
        elements = list(list_iterator)
        assert len(elements) == 0

        print("Unlocking database for listing")
        control_iterator = pyfdb.control(
            request, ControlAction.ENABLE, [ControlIdentifier.LIST]
        )
        assert control_iterator

        list_iterator = pyfdb.list(request)
        elements = list(list_iterator)
        assert len(elements) == 3


def test_control_lock_archive(read_only_fdb_setup, build_grib_messages):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        request = FDBToolRequest(
            {
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": "20200101",
                "time": "1800",
            },
        )

        print("Lock the database for archiving")
        control_iterator = pyfdb.control(
            request, ControlAction.DISABLE, [ControlIdentifier.ARCHIVE]
        )
        assert control_iterator

        elements = []

        for el in control_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1

        print("Try archiving")
        with pytest.raises(
            Exception, match=" matched for archived is LOCKED against archiving"
        ):
            pyfdb.archive(build_grib_messages.read_bytes())
            pyfdb.flush()

        print("Unlock the database for archiving")
        control_iterator = pyfdb.control(
            request, ControlAction.ENABLE, [ControlIdentifier.ARCHIVE]
        )
        assert control_iterator

        elements = []

        for el in control_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1

        print("Try archiving")
        pyfdb.archive(build_grib_messages.read_bytes())
        pyfdb.flush()
        print("Success")
