from pyfdb import FDB


def test_stats(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": "167/165/166",
        "time": "1800",
    }

    stats_iterator = fdb.stats(selection)
    assert stats_iterator

    elements = []

    for el in stats_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1
    assert "Index Statistics:" in str(elements[0])
    assert "DB Statistics:" in str(elements[0])
