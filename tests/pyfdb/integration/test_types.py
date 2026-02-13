import pytest

from pyfdb.pyfdb import FDB
from pyfdb.pyfdb_iterator import (
    ControlElement,
    IndexAxis,
    ListElement,
    MoveElement,
    PurgeElement,
    StatsElement,
    StatusElement,
    WipeElement,
)
from pyfdb.pyfdb_type import ControlAction, ControlIdentifier, DataHandle

classes = [
    IndexAxis,
    ListElement,
    StatusElement,
    MoveElement,
    PurgeElement,
    StatsElement,
    ControlElement,
    WipeElement,
    DataHandle,
]


@pytest.mark.parametrize("cls", classes)
def test_initialization_raises_error(cls):
    """Test for creation of internal data types with certain elements."""
    with pytest.raises(TypeError):
        cls(None)


def test_status_element_eq(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    print("List without lock")
    list_iterator = fdb.list(selection)
    elements = list(list_iterator)
    assert len(elements) == 3

    control_identifier_list = [
        ControlIdentifier.LIST,
        ControlIdentifier.RETRIEVE,
        ControlIdentifier.ARCHIVE,
        ControlIdentifier.WIPE,
        ControlIdentifier.UNIQUEROOT,
    ]

    print("Locking database for all control actions")
    control_iterator = fdb.control(
        selection, ControlAction.DISABLE, control_identifiers=control_identifier_list
    )
    assert control_iterator

    control_elements = list(control_iterator)

    stats_iterator = fdb.status(selection)
    assert stats_iterator

    stats_elements = list(stats_iterator)

    assert control_elements == stats_elements
