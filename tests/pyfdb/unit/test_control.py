from pathlib import Path
import yaml
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
    fdb_config_path: Path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

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

        assert not (
            fdb_config_path.parent
            / "db_store"
            / "ea:0001:oper:20200101:1800:g"
            / "retrieve.lock"
        ).exists()

        print("Locking database for retrieve")

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

        control_iterator = pyfdb.control(
            request,
            ControlAction.DISABLE,
            [ControlIdentifier.RETRIEVE],
        )
        assert control_iterator

        elements = []

        for el in control_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1

        assert (
            fdb_config_path.parent
            / "db_store"
            / "ea:0001:oper:20200101:1800:g"
            / "retrieve.lock"
        ).exists()

        print("Retrieve with lock")
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


# TODO(TKR): IS LISTING BROKEN?
@pytest.mark.skip
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

        assert (
            fdb_config_path.parent
            / "db_store"
            / "ea:0001:oper:20200101:1800:g"
            / "list.lock"
        ).exists()

        print("List with lock. Expecting 0 elements.")
        list_iterator = pyfdb.list(request)
        elements = list(list_iterator)
        assert len(elements) == 0

        print("Unlocking database for listing")
        control_iterator = pyfdb.control(
            request, ControlAction.ENABLE, [ControlIdentifier.LIST]
        )
        assert control_iterator

        # Correct behaviour is to "not see the data after locking"
        assert not (
            fdb_config_path.parent
            / "db_store"
            / "ea:0001:oper:20200101:1800:g"
            / "list.lock"
        ).exists()

        list_iterator = pyfdb.list(request)
        elements = list(list_iterator)
        assert len(elements) == 0


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

        assert (
            fdb_config_path.parent
            / "db_store"
            / "ea:0001:oper:20200101:1800:g"
            / "archive.lock"
        ).exists()

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

        assert not (
            fdb_config_path.parent
            / "db_store"
            / "ea:0001:oper:20200101:1800:g"
            / "archive.lock"
        ).exists()

        print("Try archiving")
        pyfdb.archive(build_grib_messages.read_bytes())
        pyfdb.flush()
        print("Success")


def test_control_lock_wipe(read_only_fdb_setup, build_grib_messages):
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

        print("Archive same data as the FDB was setup with in the Fixture")
        pyfdb.archive(build_grib_messages.read_bytes())
        pyfdb.flush()

        print("Lock the database for wiping")
        control_iterator = pyfdb.control(
            request, ControlAction.DISABLE, [ControlIdentifier.WIPE]
        )
        assert control_iterator

        elements = []

        for el in control_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1
        assert (
            fdb_config_path.parent
            / "db_store"
            / "ea:0001:oper:20200101:1800:g"
            / "wipe.lock"
        ).exists()

        print("Try Wipe")
        wipe_iterator = pyfdb.wipe(request, doit=True)

        elements = []

        # TODO(TKR): Ok, apparently the internal state of the iterator is borked, when we lock
        # This should either result in an error message or an empty iterator
        with pytest.raises(RuntimeError):
            for el in wipe_iterator:
                elements.append(el)

        assert len(elements) == 0

        print("Unlock the database for wiping")
        control_iterator = pyfdb.control(
            request, ControlAction.ENABLE, [ControlIdentifier.WIPE]
        )
        assert control_iterator

        elements = []

        for el in control_iterator:
            print(el)
            elements.append(el)

        assert len(elements) > 0

        assert not (
            fdb_config_path.parent
            / "db_store"
            / "ea:0001:oper:20200101:1800:g"
            / "wipe.lock"
        ).exists()

        print("Try Wipe")
        pyfdb.wipe(request, doit=True)
        pyfdb.flush()

        print("Success")


def test_enabled_per_default(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        assert pyfdb.enabled(ControlIdentifier.NONE) is True
        assert pyfdb.enabled(ControlIdentifier.LIST) is True
        assert pyfdb.enabled(ControlIdentifier.RETRIEVE) is True
        assert pyfdb.enabled(ControlIdentifier.ARCHIVE) is True
        assert pyfdb.enabled(ControlIdentifier.WIPE) is True
        assert pyfdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_disabled_writabled(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config = yaml.safe_load(fdb_config_path.read_text())
    fdb_config["writable"] = False

    fdb_config = Config(yaml.dump(fdb_config))
    pyfdb = PyFDB(fdb_config)

    assert pyfdb.enabled(ControlIdentifier.NONE) is True
    assert pyfdb.enabled(ControlIdentifier.LIST) is True
    assert pyfdb.enabled(ControlIdentifier.RETRIEVE) is True
    assert pyfdb.enabled(ControlIdentifier.ARCHIVE) is False
    assert pyfdb.enabled(ControlIdentifier.WIPE) is False
    assert pyfdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_disabled_visitable(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config = yaml.safe_load(fdb_config_path.read_text())
    fdb_config["visitable"] = False

    fdb_config = Config(yaml.dump(fdb_config))
    pyfdb = PyFDB(fdb_config)

    assert pyfdb.enabled(ControlIdentifier.NONE) is True
    assert pyfdb.enabled(ControlIdentifier.LIST) is False
    assert pyfdb.enabled(ControlIdentifier.RETRIEVE) is False
    assert pyfdb.enabled(ControlIdentifier.ARCHIVE) is True
    assert pyfdb.enabled(ControlIdentifier.WIPE) is True
    assert pyfdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_disabled_visitable_writeable(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config = yaml.safe_load(fdb_config_path.read_text())
    fdb_config["visitable"] = False
    fdb_config["writable"] = False

    fdb_config = Config(yaml.dump(fdb_config))
    pyfdb = PyFDB(fdb_config)

    assert pyfdb.enabled(ControlIdentifier.NONE) is True
    assert pyfdb.enabled(ControlIdentifier.LIST) is False
    assert pyfdb.enabled(ControlIdentifier.RETRIEVE) is False
    assert pyfdb.enabled(ControlIdentifier.ARCHIVE) is False
    assert pyfdb.enabled(ControlIdentifier.WIPE) is False
    assert pyfdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_needs_flush(empty_fdb_setup, test_data_path):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        filename = test_data_path / "x138-300.grib"

        pyfdb.archive(open(filename, "rb").read())
        assert pyfdb.needs_flush() is True
        pyfdb.flush()
        assert pyfdb.needs_flush() is False
