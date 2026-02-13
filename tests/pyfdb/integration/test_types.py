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
    """Test for creation of internal data types with certain elements."""
    with pytest.raises(TypeError):
        cls(None)


def test_status_element_eq():
    control_element_1 = StatusElement("test", _internal=True)
    control_element_2 = StatusElement("test2", _internal=True)
    control_element_3 = StatusElement("test", _internal=True)

    assert control_element_1 != control_element_2
    assert control_element_1 == control_element_3

    assert control_element_1 != "test"
