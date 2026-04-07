# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.


import datetime

import pytest

from pyfdb._internal.pyfdb_internal import FDBToolRequest


def test_from_internal_selection():
    today_date = datetime.date.today()
    yesterday = int((today_date - datetime.timedelta(days=1)).strftime("%Y%m%d"))
    fdb_tool_request = FDBToolRequest.from_internal_mars_selection({"date": ["-1"]})

    print(fdb_tool_request)
    str_repr = str(fdb_tool_request)

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
