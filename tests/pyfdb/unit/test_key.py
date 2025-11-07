from pyfdb.pyfdb_type import Key


def test_key_initialization():
    key = Key([("a", "ab"), ("b", "bb"), ("b", "bc")])

    assert str(key) == "a=ab,b=bb/bc"
