import pytest
from pyfdb._internal.pyfdb_internal import FDBToolRequest


def test_from_internal_selection():
    fdb_tool_request = FDBToolRequest.from_internal_mars_selection({"key-1": ["value-1"]})

    print(fdb_tool_request)
    str_repr = str(fdb_tool_request)

    assert "retrieve" in str_repr
    assert "key-1=value-1" in str_repr


def test_from_selection():
    # Internal representation is not respected
    with pytest.raises(ValueError):
        fdb_tool_request = FDBToolRequest.from_internal_mars_selection({"key-1": "value-1"})


def test_from_selection_all():
    fdb_tool_request = FDBToolRequest.from_internal_mars_selection({})

    print(fdb_tool_request)
    str_repr = str(fdb_tool_request)

    assert "ALL" in str_repr
