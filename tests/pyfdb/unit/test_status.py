from pyfdb.pyfdb import Config, FDBToolRequest, MarsRequest, PyFDB


def test_status(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        request = MarsRequest(
            "retrieve",
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                # "expver": "0001",
                # "stream": "oper",
                # "date": "20200101",
                # "levtype": "sfc",
                # "step": "0",
                # "param": "167/165/166",
                # "time": "1800",
            },
        )

        dump_iterator = pyfdb.status(FDBToolRequest.from_mars_request(request))

        elements = []

        for el in dump_iterator:
            elements.append(el)

        assert len(elements) == 32
