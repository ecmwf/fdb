from pyfdb import Config
from pyfdb import PyFDB
from pyfdb import FDBToolRequest


def test_stats(read_only_fdb_setup):
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

        list_iterator = pyfdb.stats(request)
        assert list_iterator

        elements = []

        for el in list_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 1
