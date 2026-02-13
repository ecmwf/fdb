from pyfdb.pyfdb import FDB


def test_status(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
    }

    status_iterator = fdb.status(selection)

    elements = []

    for el in status_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 32

    expected_key = {
        "class": ["ea"],
        "date": ["20200104"],
        "domain": ["g"],
        "expver": ["0001"],
        "stream": ["oper"],
    }
    expected_control_identifiers = []

    for element in elements:
        assert all(
            [el in element.key() for el in expected_key]
        )  # Check whether the expected key is part of the elements
        assert element.controlIdentifiers() == expected_control_identifiers
