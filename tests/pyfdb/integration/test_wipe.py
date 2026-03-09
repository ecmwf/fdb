from pyfdb import FDB
from pyfdb.pyfdb_iterator import WipeElementType, _WipeElementType


def test_wipe_enum_mapping():
    assert WipeElementType.ERROR.value == _WipeElementType.ERROR.value
    assert WipeElementType.CATALOGUE_INFO.value == _WipeElementType.CATALOGUE_INFO.value
    assert WipeElementType.CATALOGUE.value == _WipeElementType.CATALOGUE.value
    assert WipeElementType.CATALOGUE_INDEX.value == _WipeElementType.CATALOGUE_INDEX.value
    assert WipeElementType.CATALOGUE_SAFE.value == _WipeElementType.CATALOGUE_SAFE.value
    assert WipeElementType.CATALOGUE_CONTROL.value == _WipeElementType.CATALOGUE_CONTROL.value
    assert WipeElementType.STORE.value == _WipeElementType.STORE.value
    assert WipeElementType.STORE_AUX.value == _WipeElementType.STORE_AUX.value
    assert WipeElementType.STORE_SAFE.value == _WipeElementType.STORE_SAFE.value
    assert WipeElementType.UNKNOWN.value == _WipeElementType.UNKNOWN.value


def test_wipe_dryrun(read_write_fdb_setup):
    fdb = FDB(read_write_fdb_setup)

    elements = list(fdb.list({"class": "ea"}))
    assert len(elements) > 0

    wipe_iterator = fdb.wipe({"class": "ea"})
    wiped_elements = list(wipe_iterator)
    assert len(wiped_elements) > 0

    for el in wiped_elements:
        print(el)

    elements_after_wipe = list(fdb.list({"class": "ea"}))
    assert len(elements) == len(elements_after_wipe)


def test_wipe_all_doit(read_write_fdb_setup):
    fdb = FDB(read_write_fdb_setup)

    elements = list(fdb.list({"class": "ea"}))
    assert len(elements) > 0

    wipe_iterator = fdb.wipe({"class": "ea"}, doit=True)
    wiped_elements = list(wipe_iterator)
    assert len(wiped_elements) > 0

    elements_after_wipe = list(fdb.list({"class": "ea"}))
    print(f"#Elements before: {len(elements)}, Elements after: {len(elements_after_wipe)}")
    assert len(elements) > len(elements_after_wipe)


def test_wipe_single_date_doit(read_write_fdb_setup):
    fdb = FDB(read_write_fdb_setup)

    elements = list(fdb.list({"class": "ea"}))
    assert len(elements) > 0

    wipe_iterator = fdb.wipe({"class": "ea", "date": "20200101"}, doit=True)
    wiped_elements = list(wipe_iterator)
    assert len(wiped_elements) > 0

    elements_after_wipe = list(fdb.list({"class": "ea"}))
    print(f"#Elements before: {len(elements)}, Elements after: {len(elements_after_wipe)}")
    assert len(elements) > len(elements_after_wipe)
    assert len(elements) == 96
    assert len(elements_after_wipe) == 72


def populate_fdb(fdb: FDB):
    selection = {
        "class": "rd",
        "expver": "xxxx",
        "stream": "oper",
        "type": "fc",
        "date": "20000101",
        "time": "0000",
        "domain": "g",
        "levtype": "pl",
        "levelist": "300",
        "param": "138",
        "step": "0",
    }

    # Write 4 fields to the FDB based on BASE_REQUEST
    requests = [selection.copy() for _ in range(4)]

    # Modify on each of the 3 levels of the schema
    requests[1]["step"] = "1"
    requests[2]["date"] = "20000102"
    requests[3]["levtype"] = "sfc"
    del requests[3]["levelist"]

    number_of_fields = 4

    data = b"-1 Kelvin"
    for i in range(number_of_fields):
        key = requests[i]
        key = [(k, v) for k, v in key.items()]
        fdb.archive(identifier=key, data=data)
    fdb.flush()

    return number_of_fields


def test_wipe_list(empty_fdb_setup):
    fdb = FDB(empty_fdb_setup)

    number_of_fields = populate_fdb(fdb)
    assert len(list(fdb.list({}))) == number_of_fields

    # Wipe without doit: Do not actually delete anything.
    wipe_iterator = fdb.wipe({"class": "rd"})

    assert len(list(fdb.list({}))) == number_of_fields

    # Wipe, do it
    wipe_iterator = fdb.wipe({"class": "rd"}, doit=True)

    # Consume all wipe iterator elements
    for el in wipe_iterator:
        print(el.msg())
        print(el.type())
        print(el.uris())

    elements = list(fdb.list({}))
    assert len(elements) == 0
