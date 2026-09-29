# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from pyfdb import FDB, URI


def test_list_element_uri(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, level=1)
    assert list_iterator

    elements = list(list_iterator)
    assert len(elements) == 1
    print(elements[0])
    print(elements[0].uri)

    print("----------------------------------")

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, level=2)
    assert list_iterator

    elements = list(list_iterator)
    assert len(elements) == 1
    print(elements[0])
    print(elements[0].uri)

    print("----------------------------------")

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, level=3)
    assert list_iterator

    elements = [el.uri for el in list_iterator if el.uri]

    assert len(elements) == 3
    for el in elements:
        assert el is not None
        assert el.path() is not None


def test_uri_roundtrip():
    uri_str = "scheme://user:secretpass@example.com:8443/path/to/resource?query=search&sort=asc#section-2"
    test_uri = URI(uri_str)

    assert test_uri.scheme() == "scheme"
    assert test_uri.username() == "user"
    assert test_uri.password() == "secretpass"
    assert test_uri.hostname() == "example.com"
    assert test_uri.hostname() == "example.com"
    assert test_uri.netloc() == "user:secretpass@example.com:8443"
    assert test_uri.port() == 8443
    assert test_uri.path() == "/path/to/resource"
    assert test_uri.query() == "query=search&sort=asc"
    assert test_uri.fragment() == "section-2"

    assert str(test_uri) == uri_str

    print(test_uri)


def test_uri_cmp():
    uri_str = "scheme://user:secretpass@example.com:8443/path/to/resource?query=search&sort=asc#section-2"
    test_uri = URI(uri_str)

    uri_str_section3 = "scheme://user:secretpass@example.com:8443/path/to/resource?query=search&sort=asc#section-3"

    test_uri_section3 = URI(uri_str_section3)

    assert test_uri != test_uri_section3
    assert uri_str_section3 != test_uri_section3
