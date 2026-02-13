from pyfdb._internal.pyfdb_internal import FDBToolRequest


def test_from_selection():
    fdb_tool_request = FDBToolRequest.from_mars_selection({"key-1": "value-1"})

    print(fdb_tool_request)
