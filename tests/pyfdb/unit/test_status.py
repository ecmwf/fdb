from pyfdb.pyfdb import Config, FDBToolRequest, PyFDB


def test_status(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
        }

        dump_iterator = pyfdb.status(FDBToolRequest(selection))

        elements = []

        for el in dump_iterator:
            print(el)
            elements.append(el)

        assert len(elements) == 32
