from typing import List
from pyfdb.pyfdb import FDB
from pyfdb.pyfdb_iterator import StatusElement


def test_status(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

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
    expected_control_identifiers: List[StatusElement] = []

    for element in elements:
        assert all(
            [el in element.key() for el in expected_key]
        )  # Check whether the expected key is part of the elements

        assert element.controlIdentifiers() == expected_control_identifiers

        date = element.key()["date"]
        assert date and len(date) == 1
        time = element.key()["time"]
        assert time and len(time) == 1

        assert "ea:0001:oper:" in str(element.location())
        assert f":{date[0]}:" in str(element.location())
        assert f":{time[0]}:" in str(element.location())
        assert ":g" in str(element.location())

    # Test compare of status elements
    assert elements[0] == elements[0]
    assert elements[1] != elements[0]
