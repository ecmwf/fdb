import datetime

import pytest

from pyfdb._internal.pyfdb_internal import FDBToolRequest


def test_from_internal_selection():
    fdb_tool_request = FDBToolRequest.from_internal_mars_selection({"date": ["-1"]})

    print(fdb_tool_request)
    str_repr = str(fdb_tool_request)

    today_date = datetime.date.today()
    yesterday = int((today_date - datetime.timedelta(days=1)).strftime("%Y%m%d"))

    assert "retrieve" in str_repr
    assert f"date={yesterday}" in str_repr


def test_from_selection():
    # Internal representation is not respected
    with pytest.raises(ValueError, match="Expecting collection of values"):
        _ = FDBToolRequest.from_internal_mars_selection({"date": "-1"})


def test_from_selection_all():
    fdb_tool_request = FDBToolRequest.from_internal_mars_selection({})

    print(fdb_tool_request)
    str_repr = str(fdb_tool_request)

    assert "ALL" in str_repr
