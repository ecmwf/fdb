# Cases where we support wildcard selection on the C++ API
#
# list
# status
# wipe
# purge
# stats
# control
# axes

from pathlib import Path
from pyfdb.pyfdb import FDB
from pyfdb.pyfdb_iterator import ControlElement, IndexAxis
from pyfdb.pyfdb_type import ControlAction, ControlIdentifier, WildcardMarsSelection


def test_list_wildcard(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)
    selection = WildcardMarsSelection()
    print(f"Stringified selection:\n  {selection}")
    list_iterator = fdb.list(selection, level=1)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 32

    print("----------------------------------")

    list_iterator = fdb.list(selection, level=2)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 32

    print("----------------------------------")

    list_iterator = fdb.list(selection, level=3)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        print(el.uri())
        elements.append(el)

    assert len(elements) == 96


def test_status_wildcard(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)
    selection = WildcardMarsSelection()
    status_iterator = fdb.status(selection)

    elements = []

    for el in status_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 32


def test_wipe_dryrun_wildcard(read_write_fdb_setup):
    fdb = FDB(read_write_fdb_setup)
    elements = list(fdb.list(WildcardMarsSelection()))
    initial_len = len(elements)

    wipe_iterator = fdb.wipe(WildcardMarsSelection())
    wiped_elements = list(wipe_iterator)
    assert len(wiped_elements) > initial_len

    for el in wiped_elements:
        print(el)

    elements_after_wipe = list(fdb.list(WildcardMarsSelection()))
    assert initial_len == len(elements_after_wipe)


def test_wipe_all_doit_wildcard(read_write_fdb_setup):
    fdb_config_path = read_write_fdb_setup

    fdb = FDB(fdb_config_path)

    elements = list(fdb.list(WildcardMarsSelection()))
    assert len(elements) > 0

    wipe_iterator = fdb.wipe(WildcardMarsSelection(), doit=True)
    wiped_elements = list(wipe_iterator)
    assert len(wiped_elements) > 0

    elements_after_wipe = list(fdb.list(WildcardMarsSelection()))
    print(
        f"#Elements before: {len(elements)}, Elements after: {len(elements_after_wipe)}"
    )
    assert len(elements_after_wipe) == 0


def test_purge_wildcard(empty_fdb_setup, build_grib_messages):
    fdb = FDB(empty_fdb_setup)

    initial_elements = list(fdb.list(WildcardMarsSelection()))

    assert len(initial_elements) == 0
    print(f"Elements initial: {len(initial_elements)}")

    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    archived_elements = list(fdb.list(WildcardMarsSelection()))
    assert len(archived_elements) == 96

    print(f"Elements after archive: {len(archived_elements)}")

    purge_iterator = fdb.purge(WildcardMarsSelection(), doit=True)
    purged_output = []

    for el in purge_iterator:
        print(el)
        purged_output.append(el)

    assert len(purged_output) > 0

    elements_after_first_purge = list(fdb.list(WildcardMarsSelection()))

    print(f"Elements after purge: {len(elements_after_first_purge)}")
    assert len(archived_elements) == len(elements_after_first_purge)

    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    after_archive_elements = list(fdb.list(WildcardMarsSelection()))

    print(
        f"Elements after masking archive: {len(after_archive_elements)}, before: {len(initial_elements)}"
    )
    assert len(after_archive_elements) > len(initial_elements)

    purge_iterator = fdb.purge(WildcardMarsSelection(), doit=True)
    purged_output = list(purge_iterator)

    assert len(purged_output) > 0

    elements_after_second_purge = list(fdb.list(WildcardMarsSelection()))

    for el in elements_after_second_purge:
        print(el)

    print(f"Elements after second purge: {len(elements_after_second_purge)}")
    assert len(archived_elements) == len(elements_after_second_purge)


def test_stats_wildcard(read_only_fdb_setup):
    assert read_only_fdb_setup
    fdb = FDB(read_only_fdb_setup)

    list_iterator = fdb.stats(WildcardMarsSelection())
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 32


def test_control_lock_retrieve_wildcard(read_only_fdb_setup):
    assert read_only_fdb_setup
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
        read_only_fdb_setup.parent
        / "db_store"
        / "ea:0001:oper:20200101:1800:g"
        / "retrieve.lock"
    ).exists()

    print("Locking entire database for retrieve")

    control_iterator = fdb.control(
        WildcardMarsSelection(),
        ControlAction.DISABLE,
        [ControlIdentifier.RETRIEVE],
    )
    assert control_iterator

    elements = []

    el: ControlElement

    for el in control_iterator:
        assert (Path(el.location().path()) / "retrieve.lock").exists()
        elements.append(el)

    assert len(elements) == 32

    # Needed because reuse of databases in the retrieve path is using a cached db
    # object which contains the first (non-locked) status of the db.
    # This can be removed as soon as https://jira.ecmwf.int/browse/FDB-613 is fixed.
    fdb = FDB(read_only_fdb_setup)

    with fdb.retrieve(
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
    ) as data_handle_2:
        assert data_handle_2
        assert data_handle_2.read(4) == b"\x00\x00\x00\x00"


def test_index_axis_items_levels_wildcard(read_only_fdb_setup):
    assert read_only_fdb_setup
    fdb = FDB(read_only_fdb_setup)
    index_axis: IndexAxis = fdb.axes(WildcardMarsSelection())

    assert len(index_axis) == 11

    print("---------- Level 3: ----------")

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    for k, v in index_axis.items():
        assert index_axis.has_key(k)

    index_axis: IndexAxis = fdb.axes(WildcardMarsSelection(), level=2)

    assert len(index_axis) == 8

    print("---------- Level 2: ----------")

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    index_axis: IndexAxis = fdb.axes(WildcardMarsSelection(), level=1)

    assert len(index_axis) == 6

    print("---------- Level 1: ----------")

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")
