from pyfdb import Config, PyFDB
from pyfdb.pyfdb import FDBToolRequest


def test_list(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        request = FDBToolRequest(
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
        print(f"Stringified tool request:\n  {request}")

        list_iterator = pyfdb.list(request, level=1)
        assert list_iterator

        elements = []

        for el in list_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1

        print("----------------------------------")

        request = FDBToolRequest(
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": "20200101",
                "levtype": "sfc",
                "step": "0",
                "time": "1800",
            },
        )

        list_iterator = pyfdb.list(request, level=2)
        assert list_iterator

        elements = []

        for el in list_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1

        print("----------------------------------")

        request = FDBToolRequest(
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": "20200101",
                "levtype": "sfc",
                "step": "0",
                "time": "1800",
            },
        )

        list_iterator = pyfdb.list(request, level=3)
        assert list_iterator

        elements = []

        for el in list_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 3


def test_list_deduplicate(read_only_fdb_setup, build_grib_messages):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        request = FDBToolRequest(
            {
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
            },
        )

        list_iterator = pyfdb.list(request, duplicates=False, level=3)
        assert list_iterator

        elements = []

        for el in list_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1

        print("Archiving duplicate data")
        pyfdb.archive(build_grib_messages.read_bytes())
        pyfdb.flush()

        list_iterator = pyfdb.list(request, duplicates=True, level=3)
        assert list_iterator
        elements = []

        for el in list_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 2
