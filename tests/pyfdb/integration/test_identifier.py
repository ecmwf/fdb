import pytest

from pyfdb.pyfdb_type import Identifier


def test_identifier_initialization():
    identifier = Identifier([("a", "ab"), ("b", "bb"), ("c", "bc")])
    assert identifier


def test_identifier_stringify():
    identifier = Identifier([("a", "ab"), ("b", "bb"), ("c", "bc")])
    assert identifier
    print(str(identifier))


def test_identifier_initialization_double_key():
    with pytest.raises(KeyError, match="already exists"):
        Identifier([("a", "ab"), ("b", "bb"), ("b", "bc"), ("a", "bc")])
