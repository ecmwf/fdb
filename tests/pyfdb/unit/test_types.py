import pytest

from pyfdb.pyfdb_iterator import (
    ControlElement,
    IndexAxis,
    ListElement,
    MoveElement,
    PurgeElement,
    StatsElement,
    StatusElement,
)

classes = [
    IndexAxis,
    ListElement,
    StatusElement,
    MoveElement,
    PurgeElement,
    StatsElement,
    ControlElement,
]


@pytest.mark.parametrize("cls", classes)
def test_initialization_raises_error(cls):
    with pytest.raises(NotImplementedError):
        cls()
