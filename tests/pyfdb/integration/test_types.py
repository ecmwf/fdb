import pytest

from pyfdb.pyfdb_iterator import (
    ControlElement,
    IndexAxis,
    ListElement,
    PurgeElement,
    StatsElement,
    StatusElement,
    WipeElement,
)
from pyfdb.pyfdb_type import DataHandle

classes = [
    IndexAxis,
    ListElement,
    StatusElement,
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
