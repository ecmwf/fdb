from pyfdb import Config, MarsRequest, PyFDB


def test_read(read_only_fdb_setup):
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
                "expver": "0001",
                "stream": "oper",
                "date": "20200101",
                "levtype": "sfc",
                "step": "0",
                "param": "167/165/166",
                "time": "1800",
            },
        )

        list_iterator = pyfdb.inspect(request)

        elements = []

        for el in list_iterator:
            elements.append(el)

        for el in list_iterator:
            data_handle = el.dataHandle()
            assert data_handle
            assert data_handle.read(4) == b"GRIB"


# TODO(TKR) the other two implementations for read
