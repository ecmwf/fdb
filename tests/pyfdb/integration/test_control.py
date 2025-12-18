from pathlib import Path
import yaml
from pyfdb import FDB, ControlAction, ControlIdentifier

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

    fdb = FDB(fdb_config_path.read_text())

    print("Retrieve without lock")
    data_handle = fdb.retrieve(
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
        }
    )
    assert data_handle
    data_handle.open()
    assert data_handle.read(4) == b"GRIB"
    data_handle.close()

    assert not (
        fdb_config_path.parent
        / "db_store"
        / "ea:0001:oper:20200101:1800:g"
        / "retrieve.lock"
    ).exists()

    print("Locking database for retrieve")

    selection = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    control_iterator = fdb.control(
        selection,
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
    data_handle = fdb.retrieve(
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
    assert data_handle
    data_handle.open()
    assert data_handle.read(4) == b"GRIB"
    data_handle.close()


def test_control_lock_list(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path.read_text())

    selection = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    print("List without lock")
    list_iterator = fdb.list(selection)
    elements = list(list_iterator)
    assert len(elements) == 3

    print("Locking database for listing")
    control_iterator = fdb.control(
        selection, ControlAction.DISABLE, [ControlIdentifier.LIST]
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
    list_iterator = fdb.list(selection)
    elements = list(list_iterator)
    assert len(elements) == 0

    print("Unlocking database for listing")
    control_iterator = fdb.control(
        selection, ControlAction.ENABLE, [ControlIdentifier.LIST]
    )
    assert control_iterator

    elements = list(control_iterator)
    assert len(elements) > 0

    # Correct behaviour is to "not see the data after locking"
    assert not (
        fdb_config_path.parent
        / "db_store"
        / "ea:0001:oper:20200101:1800:g"
        / "list.lock"
    ).exists()

    list_iterator = fdb.list(selection)
    elements = list(list_iterator)
    assert len(elements) == 3


def test_control_lock_archive(read_only_fdb_setup, build_grib_messages):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path.read_text())

    selection = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    print("Lock the database for archiving")
    control_iterator = fdb.control(
        selection, ControlAction.DISABLE, [ControlIdentifier.ARCHIVE]
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
        fdb.archive(build_grib_messages.read_bytes())
        fdb.flush()

    print("Unlock the database for archiving")
    control_iterator = fdb.control(
        selection, ControlAction.ENABLE, [ControlIdentifier.ARCHIVE]
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
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()
    print("Success")


def test_control_lock_archive_status(read_only_fdb_setup, build_grib_messages):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path.read_text())

    selection = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    print("Lock the database for archiving")
    control_iterator = fdb.control(
        selection, ControlAction.DISABLE, [ControlIdentifier.ARCHIVE]
    )
    assert control_iterator

    elements = []

    for el in control_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1

    print("--------- OUTPUT STATUS -----------")

    status_iterator = fdb.status(selection)

    elements = list(status_iterator)

    for el in elements:
        print(el)

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
        fdb.archive(build_grib_messages.read_bytes())
        fdb.flush()

    print("Unlock the database for archiving")
    control_iterator = fdb.control(
        selection, ControlAction.ENABLE, [ControlIdentifier.ARCHIVE]
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
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()
    print("Success")


def test_control_lock_wipe(read_only_fdb_setup, build_grib_messages):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path.read_text())

    selection = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    print("Archive same data as the FDB was setup with in the Fixture")
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    print("Lock the database for wiping")
    control_iterator = fdb.control(
        selection, ControlAction.DISABLE, [ControlIdentifier.WIPE]
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
    wipe_iterator = fdb.wipe(selection, doit=True)

    elements = []

    with pytest.raises(RuntimeError):
        for el in wipe_iterator:
            elements.append(el)

    assert len(elements) == 0

    print("Unlock the database for wiping")
    control_iterator = fdb.control(
        selection, ControlAction.ENABLE, [ControlIdentifier.WIPE]
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
    fdb.wipe(selection, doit=True)
    fdb.flush()

    print("Success")


def test_enabled_per_default(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path.read_text())

    assert fdb.enabled(ControlIdentifier.NONE) is True
    assert fdb.enabled(ControlIdentifier.LIST) is True
    assert fdb.enabled(ControlIdentifier.RETRIEVE) is True
    assert fdb.enabled(ControlIdentifier.ARCHIVE) is True
    assert fdb.enabled(ControlIdentifier.WIPE) is True
    assert fdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_disabled_writabled(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config = yaml.safe_load(fdb_config_path.read_text())
    fdb_config["writable"] = False

    fdb = FDB(yaml.dump(fdb_config))

    assert fdb.enabled(ControlIdentifier.NONE) is True
    assert fdb.enabled(ControlIdentifier.LIST) is True
    assert fdb.enabled(ControlIdentifier.RETRIEVE) is True
    assert fdb.enabled(ControlIdentifier.ARCHIVE) is False
    assert fdb.enabled(ControlIdentifier.WIPE) is False
    assert fdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_disabled_visitable(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config = yaml.safe_load(fdb_config_path.read_text())
    fdb_config["visitable"] = False

    fdb = FDB(yaml.dump(fdb_config))

    assert fdb.enabled(ControlIdentifier.NONE) is True
    assert fdb.enabled(ControlIdentifier.LIST) is False
    assert fdb.enabled(ControlIdentifier.RETRIEVE) is False
    assert fdb.enabled(ControlIdentifier.ARCHIVE) is True
    assert fdb.enabled(ControlIdentifier.WIPE) is True
    assert fdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_disabled_visitable_writeable(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config = yaml.safe_load(fdb_config_path.read_text())
    fdb_config["visitable"] = False
    fdb_config["writable"] = False

    fdb = FDB(yaml.dump(fdb_config))

    assert fdb.enabled(ControlIdentifier.NONE) is True
    assert fdb.enabled(ControlIdentifier.LIST) is False
    assert fdb.enabled(ControlIdentifier.RETRIEVE) is False
    assert fdb.enabled(ControlIdentifier.ARCHIVE) is False
    assert fdb.enabled(ControlIdentifier.WIPE) is False
    assert fdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_needs_flush(empty_fdb_setup, test_data_path):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    filename = test_data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read())
    assert fdb.needs_flush() is True
    fdb.flush()
    assert fdb.needs_flush() is False
