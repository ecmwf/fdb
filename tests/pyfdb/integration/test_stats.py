# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from pyfdb import FDB


def test_stats(read_only_fdb_setup):
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
    assert "Fields" in stats
    assert "Size of fields" in stats
    assert "Reacheable fields" in stats
    assert "Reachable size" in stats
    assert "Databases" in stats
    assert "TOC records" in stats
    assert "Size of TOC files" in stats
    assert "Size of schemas files" in stats
    assert "TOC records" in stats
    assert "Owned data files" in stats
    assert "Size of owned data files" in stats
    assert "Index files" in stats
    assert "Size of index files" in stats
    assert "Size of TOC files" in stats
    assert "Total owned size" in stats
    assert "Total size" in stats


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
    assert "Index Statistics:" not in stats
    assert "Fields                          : 3" not in stats
    assert "Reacheable fields               : 3" not in stats
    assert "DB Statistics:" not in stats
    assert "Databases                       : 1" in stats
    assert "TOC records                     : 2" in stats
    assert "TOC records                     : 2" in stats
    assert "Owned data files                : 1" in stats
    assert "Index files                     : 1" in stats

    # Check fields
    assert "TOC records" in stats
    assert "Size of TOC files" in stats
    assert "Size of schemas files" in stats
    assert "TOC records" in stats
    assert "Owned data files" in stats
    assert "Size of owned data files" in stats
    assert "Index files" in stats
    assert "Size of index files" in stats
    assert "Size of TOC files" in stats
    assert "Total owned size" in stats
    assert "Total size" in stats


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
    assert "DB Statistics:" not in stats
    assert "Databases                       : 1" not in stats
    assert "TOC records                     : 2" not in stats
    assert "TOC records                     : 2" not in stats
    assert "Owned data files                : 1" not in stats
    assert "Index files                     : 1" not in stats

    # Check fields
    assert "Fields" in stats
    assert "Size of fields" in stats
    assert "Reacheable fields" in stats
    assert "Reachable size" in stats
