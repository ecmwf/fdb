import pytest

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
from pyfdb.pyfdb_type import URI, DataHandle

classes = [
    IndexAxis,
    ListElement,
    StatusElement,
    MoveElement,
    PurgeElement,
    StatsElement,
    ControlElement,
    WipeElement,
    URI,
    DataHandle,
]


@pytest.mark.parametrize("cls", classes)
def test_initialization_raises_error(cls):
    with pytest.raises(NotImplementedError):
        cls()


def test_status_element_eq():
    control_element_1 = StatusElement._from_raw("test")
    control_element_2 = StatusElement._from_raw("test2")
    control_element_3 = StatusElement._from_raw("test")

    assert control_element_1 != control_element_2
    assert control_element_1 == control_element_3

    assert control_element_1 != "test"
