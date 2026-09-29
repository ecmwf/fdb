# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from pathlib import Path

import pytest
import yaml

from pyfdb import FDB, ControlAction, ControlIdentifier
from pyfdb._internal import _ControlAction, _ControlIdentifier
from pyfdb.pyfdb_type import UserInputMapper


def test_control_action_values():
    assert ControlAction.NONE.value == _ControlAction.NONE.value
    assert ControlAction.DISABLE.value == _ControlAction.DISABLE.value
    assert ControlAction.ENABLE.value == _ControlAction.ENABLE.value


def test_control_identifier_values():
    assert _ControlIdentifier.NONE.value == ControlIdentifier.NONE.value
    assert _ControlIdentifier.LIST.value == ControlIdentifier.LIST
    assert _ControlIdentifier.RETRIEVE.value == ControlIdentifier.RETRIEVE
    assert _ControlIdentifier.ARCHIVE.value == ControlIdentifier.ARCHIVE
    assert _ControlIdentifier.WIPE.value == ControlIdentifier.WIPE
    assert _ControlIdentifier.UNIQUEROOT.value == ControlIdentifier.UNIQUEROOT


def test_control_identifier_string():
    assert str(ControlIdentifier.NONE) == "NONE"
    assert str(ControlIdentifier.LIST) == "LIST"
    assert str(ControlIdentifier.RETRIEVE) == "RETRIEVE"
    assert str(ControlIdentifier.ARCHIVE) == "ARCHIVE"
    assert str(ControlIdentifier.WIPE) == "WIPE"
    assert str(ControlIdentifier.UNIQUEROOT) == "UNIQUEROOT"

    assert str(ControlIdentifier.NONE | ControlIdentifier.LIST) == "LIST"
    assert str(ControlIdentifier.LIST | ControlIdentifier.RETRIEVE) == "LIST|RETRIEVE"
    assert str(ControlIdentifier.RETRIEVE | ControlIdentifier.NONE) == "RETRIEVE"
    assert (
        str(
            ControlIdentifier.RETRIEVE | ControlIdentifier.NONE | ControlIdentifier.LIST
        )
        == "LIST|RETRIEVE"
    )


def test_control_action_string():
    assert str(ControlAction.NONE) == "NONE"
    assert str(ControlAction.ENABLE) == "ENABLE"
    assert str(ControlAction.DISABLE) == "DISABLE"


def test_control_lock_retrieve(read_only_fdb_setup, function_tmp):
    fdb = FDB(read_only_fdb_setup)

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
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "retrieve.lock"
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
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "retrieve.lock"
    ).exists()

    print("Retrieve with lock")
    # Needed because reuse of databases in the retrieve path is using a cached db
    # object which contains the first (non-locked) status of the db.
    # This can be removed as soon as https://jira.ecmwf.int/browse/FDB-613 is fixed.
    fdb = FDB(read_only_fdb_setup)

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
    assert data_handle.read(4) == b""
    data_handle.close()


def test_control_lock_list(read_only_fdb_setup, function_tmp):
    fdb = FDB(read_only_fdb_setup)

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
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "list.lock"
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
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "list.lock"
    ).exists()

    list_iterator = fdb.list(selection)
    elements = list(list_iterator)
    assert len(elements) == 3


def test_control_lock_archive(read_only_fdb_setup, build_grib_messages, function_tmp):
    fdb = FDB(read_only_fdb_setup)

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

    elements = []

    for el in control_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1

    assert (
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "archive.lock"
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
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "archive.lock"
    ).exists()

    print("Try archiving")
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()
    print("Success")


def test_control_lock_archive_status(
    read_only_fdb_setup, build_grib_messages, function_tmp, test_logger
):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    test_logger.info("Lock the database for archiving")
    control_iterator = fdb.control(
        selection, ControlAction.DISABLE, [ControlIdentifier.ARCHIVE]
    )
    assert control_iterator

    elements = []

    for el in control_iterator:
        test_logger.debug(el)
        elements.append(el)

    assert len(elements) == 1

    test_logger.debug("--------- OUTPUT STATUS -----------")

    status_iterator = fdb.status(selection)

    elements = list(status_iterator)

    for el in elements:
        test_logger.debug(el)

    assert (
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "archive.lock"
    ).exists()

    test_logger.info("Try archiving")
    with pytest.raises(
        Exception, match=" matched for archived is LOCKED against archiving"
    ):
        fdb.archive(build_grib_messages.read_bytes())
        fdb.flush()

    test_logger.info("Unlock the database for archiving")
    control_iterator = fdb.control(
        selection, ControlAction.ENABLE, [ControlIdentifier.ARCHIVE]
    )
    assert control_iterator

    elements = []

    for el in control_iterator:
        test_logger.debug(el)
        elements.append(el)

    assert len(elements) == 1

    assert not (
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "archive.lock"
    ).exists()

    test_logger.info("Try archiving")
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()
    test_logger.info("Success")


def test_control_lock_wipe(read_only_fdb_setup, function_tmp, build_grib_messages):
    fdb = FDB(read_only_fdb_setup)

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
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "wipe.lock"
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
        function_tmp / "db_store" / "ea:0001:oper:20200101:1800:g" / "wipe.lock"
    ).exists()

    print("Try Wipe")
    fdb.wipe(selection, doit=True)
    fdb.flush()

    print("Success")


def test_enabled_per_default(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    assert fdb.enabled(ControlIdentifier.NONE) is True
    assert fdb.enabled(ControlIdentifier.LIST) is True
    assert fdb.enabled(ControlIdentifier.RETRIEVE) is True
    assert fdb.enabled(ControlIdentifier.ARCHIVE) is True
    assert fdb.enabled(ControlIdentifier.WIPE) is True
    assert fdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_disabled_writable(read_only_fdb_setup):
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
    fdb_config = yaml.safe_load(read_only_fdb_setup.read_text())
    fdb_config["visitable"] = False

    fdb = FDB(yaml.dump(fdb_config))

    assert fdb.enabled(ControlIdentifier.NONE) is True
    assert fdb.enabled(ControlIdentifier.LIST) is False
    assert fdb.enabled(ControlIdentifier.RETRIEVE) is False
    assert fdb.enabled(ControlIdentifier.ARCHIVE) is True
    assert fdb.enabled(ControlIdentifier.WIPE) is True
    assert fdb.enabled(ControlIdentifier.UNIQUEROOT) is True


def test_disabled_visitable_writeable(read_only_fdb_setup):
    fdb_config = yaml.safe_load(read_only_fdb_setup.read_text())
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
    fdb = FDB(empty_fdb_setup)

    filename: Path = test_data_path / "x138-300.grib"

    fdb.archive(filename.read_bytes())
    assert fdb.dirty() is True
    fdb.flush()
    assert fdb.dirty() is False


def test_control_element_key(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

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
        elements.append(el.key())

    assert len(elements) == 1
    print(f"Element key: {elements[0]}")
    print(f"Element key type: {type(elements[0])}")
    assert elements[0] == UserInputMapper.map_selection_to_internal(selection)


def test_control_element_control_identifiers(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

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

    control_identifier_list = [
        ControlIdentifier.LIST,
        ControlIdentifier.RETRIEVE,
        ControlIdentifier.ARCHIVE,
        ControlIdentifier.WIPE,
        ControlIdentifier.UNIQUEROOT,
    ]

    print("Locking database for all control actions")
    control_iterator = fdb.control(
        selection, ControlAction.DISABLE, control_identifiers=control_identifier_list
    )
    assert control_iterator

    elements = []

    for el in control_iterator:
        print(el)
        elements.append(el.controlIdentifiers())

    assert len(elements) == 1
    assert (
        len(elements[0]) == 5
    )  # There should be 5 control identifiers for the first database
    assert elements[0] == control_identifier_list
    print(f"Element control identifiers: {elements[0]}")
    print(f"Element control identifiers type: {type(elements[0])}")

    print("Unlocking database for all control actions")
    control_iterator = fdb.control(
        selection, ControlAction.ENABLE, control_identifiers=control_identifier_list
    )
    assert control_iterator

    elements = [el.controlIdentifiers() for el in control_iterator]

    assert len(elements) == 1
    assert len(elements[0]) == 0
    print(f"Element control identifiers: {elements[0]}")
    print(f"Element control identifiers type: {type(elements[0])}")


def test_status_element_eq_control_element(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

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

    control_identifier_list = [
        ControlIdentifier.LIST,
        ControlIdentifier.RETRIEVE,
        ControlIdentifier.ARCHIVE,
        ControlIdentifier.WIPE,
        ControlIdentifier.UNIQUEROOT,
    ]

    print("Locking all databases for all control actions")

    # Control results for all databases
    control_iterator = fdb.control(
        {}, ControlAction.DISABLE, control_identifiers=control_identifier_list
    )
    assert control_iterator

    control_elements = list(control_iterator)

    # Status for all databases
    status_iterator = fdb.status({})
    assert status_iterator

    status_elements = list(status_iterator)
    assert control_elements == status_elements

    for i in range(len(control_elements)):
        print(f"Control Element: \n {control_elements[i]}")
        print(f"Status Element: \n {status_elements[i]}")

    # Check (cross-verify) that consecutive elements are different
    for i in range(len(control_elements) - 1):
        assert control_elements[i + 1] != status_elements[i]
