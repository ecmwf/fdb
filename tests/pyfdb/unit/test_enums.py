import pytest

from pyfdb._internal import ControlAction as _ControlAction
from pyfdb._internal import ControlIdentifier as _ControlIdentifier
from pyfdb.pyfdb_type import ControlAction, ControlIdentifier


def test_enum_control_identifiers():
    assert ControlIdentifier.NONE == 0
    assert ControlIdentifier.LIST == 1
    assert ControlIdentifier.RETRIEVE == 2
    assert ControlIdentifier.ARCHIVE == 4
    assert ControlIdentifier.WIPE == 8
    assert ControlIdentifier.UNIQUEROOT == 16


control_identifiers_enum_variants = [
    (_ControlIdentifier.NONE, ControlIdentifier.NONE),
    (_ControlIdentifier.LIST, ControlIdentifier.LIST),
    (_ControlIdentifier.RETRIEVE, ControlIdentifier.RETRIEVE),
    (_ControlIdentifier.ARCHIVE, ControlIdentifier.ARCHIVE),
    (_ControlIdentifier.WIPE, ControlIdentifier.WIPE),
    (_ControlIdentifier.UNIQUEROOT, ControlIdentifier.UNIQUEROOT),
]


@pytest.mark.parametrize("variant, expected", control_identifiers_enum_variants)
def test_enum_control_identifiers_from_raw(variant, expected):
    assert ControlIdentifier._from_raw(variant) == expected


control_action_enum_variants = [
    (_ControlAction.NONE, ControlAction.NONE),
    (_ControlAction.DISABLE, ControlAction.DISABLE),
    (_ControlAction.ENABLE, ControlAction.ENABLE),
]


@pytest.mark.parametrize("variant, expected", control_action_enum_variants)
def test_enum_control_actions_from_raw(variant, expected):
    assert ControlAction._from_raw(variant) == expected
