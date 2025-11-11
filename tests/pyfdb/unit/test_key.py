from pyfdb.pyfdb_type import Key


# TODO(TKR): A key can never have a range of values
def test_key_initialization():
    key = Key([("a", "ab"), ("b", "bb"), ("b", "bc"), ("a", "bc")])

    assert str(key) == "a=ab/bc,b=bb/bc"
