from pyfdb import FDB


def test_list(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }
    print(f"Stringified selection:\n  {selection}")

    list_iterator = fdb.list(selection, level=1)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1

    print("----------------------------------")

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, level=2)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1

    print("----------------------------------")

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, level=3)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        print(el.uri())
        elements.append(el)

    assert len(elements) == 3


def test_list_deduplicate(read_only_fdb_setup, build_grib_messages):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": "167",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, duplicates=False, level=3)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1

    print("Archiving duplicate data")
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    list_iterator = fdb.list(selection, duplicates=True, level=3)
    assert list_iterator
    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 2


def test_list_read_from_datahandle(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
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
    print(f"Stringified selection:\n  {selection}")

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, level=3)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 3

    for el in elements:
        data_handle = el.data_handle()
        data_handle.open()
        assert data_handle.read(4) == b"GRIB"
        data_handle.close()
