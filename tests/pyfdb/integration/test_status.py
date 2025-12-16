from pyfdb.pyfdb import PyFDB


def test_status(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    pyfdb = PyFDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
    }

    status_iterator = pyfdb.status(selection)

    elements = []

    for el in status_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 32
