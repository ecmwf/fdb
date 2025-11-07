import time
from pyfdb.pyfdb import Config, FDBToolRequest, MarsRequest, PyFDB


import pytest


def test_dump(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        request = MarsRequest(
            "retrieve",
            {
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": "20200101",
                "time": "1800",
            },
        )

        dump_iterator = pyfdb.dump(
            FDBToolRequest.from_mars_request(request), simple=False
        )

        elements = []

        print("Before loop")
        for el in dump_iterator:
            elements.append(el)
            print(el)

        assert len(elements) == 0
