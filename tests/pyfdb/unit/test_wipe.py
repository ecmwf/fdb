from gitdb.util import time

from pyfdb import Config, PyFDB
from pyfdb.pyfdb import FDBToolRequest


def test_wipe(read_write_fdb_setup):
    fdb_config_path = read_write_fdb_setup

    fdb_config = Config(fdb_config_path.read_text())
    pyfdb = PyFDB(fdb_config)

    elements = list(pyfdb.list(FDBToolRequest(key_values={"class": "ea"})))
    assert len(elements) > 0

    wipe_iterator = pyfdb.wipe(FDBToolRequest(key_values={"class": "ea"}))
    elements = list(wipe_iterator)
    assert len(elements) > 0

    elements_after_wipe = list(pyfdb.list(FDBToolRequest(key_values={"class": "ea"})))
    assert len(elements) > len(elements_after_wipe)


BASE_REQUEST = {
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


def populate_fdb(fdb: PyFDB):
    # Write 4 fields to the FDB based on BASE_REQUEST
    requests = [BASE_REQUEST.copy() for i in range(4)]

    # Modify on each of the 3 levels of the schema
    requests[1]["step"] = "1"
    requests[2]["date"] = "20000102"
    requests[3]["levtype"] = "sfc"
    del requests[3]["levelist"]

    NFIELDS = 4

    data = b"-1 Kelvin"
    for i in range(NFIELDS):
        key = requests[i]
        key = ",".join([f"{k}={v}" for k, v in key.items()])
        fdb.archive_key(key=key, bytes=data)
    fdb.flush()

    return NFIELDS


def test_wipe_list(empty_fdb_setup):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        NFIELDS = populate_fdb(pyfdb)
        assert len([x for x in pyfdb.list(FDBToolRequest(all=True))]) == NFIELDS

        # Wipe without doit: Do not actually delete anything.
        wipe_iterator = pyfdb.wipe(FDBToolRequest(key_values={"class": "rd"}))

        # Consume all wipe iterator elements
        for el in wipe_iterator:
            pass

        assert len([x for x in pyfdb.list(FDBToolRequest(all=True))]) == NFIELDS

        # Wipe, do it
        wipe_iterator = pyfdb.wipe(
            FDBToolRequest(key_values={"class": "rd"}), doit=True
        )

        # Consume all wipe iterator elements
        for el in wipe_iterator:
            pass

        list_iterator = pyfdb.list(FDBToolRequest(all=True))
        elements = [x for x in list_iterator]

        assert len(elements) == 0
