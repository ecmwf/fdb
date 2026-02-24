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
    stats = str(elements[0])

    # Check for database and index statistics
    assert "Index Statistics:" in stats
    assert "Fields                          : 3" in stats
    assert "Reacheable fields               : 3" in stats
    assert "DB Statistics:" in stats
    assert "Databases                       : 1" in stats
    assert "TOC records                     : 2" in stats
    assert "TOC records                     : 2" in stats
    assert "Owned data files                : 1" in stats
    assert "Index files                     : 1" in stats

    # Check fields
    "Fields" in stats
    "Size of fields" in stats
    "Reacheable fields" in stats
    "Reachable size" in stats
    "Databases" in stats
    "TOC records" in stats
    "Size of TOC files" in stats
    "Size of schemas files" in stats
    "TOC records" in stats
    "Owned data files" in stats
    "Size of owned data files" in stats
    "Index files" in stats
    "Size of index files" in stats
    "Size of TOC files" in stats
    "Total owned size" in stats
    "Total size" in stats


def test_stats_db_stats(read_only_fdb_setup):
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
        "param": "167/165/166",
        "time": "1800",
    }

    stats_iterator = fdb.stats(selection)
    assert stats_iterator

    elements = list(stats_iterator)

    assert len(elements) == 1
    stats = elements[0].db_statistics()

    print(stats)

    # Check for database and index statistics
    assert not "Index Statistics:" in stats
    assert not "Fields                          : 3" in stats
    assert not "Reacheable fields               : 3" in stats
    assert not "DB Statistics:" in stats
    assert "Databases                       : 1" in stats
    assert "TOC records                     : 2" in stats
    assert "TOC records                     : 2" in stats
    assert "Owned data files                : 1" in stats
    assert "Index files                     : 1" in stats

    # Check fields
    "TOC records" in stats
    "Size of TOC files" in stats
    "Size of schemas files" in stats
    "TOC records" in stats
    "Owned data files" in stats
    "Size of owned data files" in stats
    "Index files" in stats
    "Size of index files" in stats
    "Size of TOC files" in stats
    "Total owned size" in stats
    "Total size" in stats


def test_stats_index_stats(read_only_fdb_setup):
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
        "param": "167/165/166",
        "time": "1800",
    }

    stats_iterator = fdb.stats(selection)
    assert stats_iterator

    elements = list(stats_iterator)

    assert len(elements) == 1
    stats = elements[0].index_statistics()

    print(stats)

    # Check for database and index statistics
    assert "Fields                          : 3" in stats
    assert "Reacheable fields               : 3" in stats
    assert not "DB Statistics:" in stats
    assert not "Databases                       : 1" in stats
    assert not "TOC records                     : 2" in stats
    assert not "TOC records                     : 2" in stats
    assert not "Owned data files                : 1" in stats
    assert not "Index files                     : 1" in stats

    # Check fields
    "Fields" in stats
    "Size of fields" in stats
    "Reacheable fields" in stats
    "Reachable size" in stats
