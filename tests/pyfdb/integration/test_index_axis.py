import pytest
from pyfdb.pyfdb import FDB
from pyfdb.pyfdb_iterator import IndexAxis
from pyfdb.pyfdb_type import WildcardMarsSelection


def test_index_axis_string(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

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

    index_axis = fdb.axes(selection)

    assert index_axis
    assert str(index_axis)


def test_index_axis_get(read_only_fdb_setup):
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

    index_axis: IndexAxis = fdb.axes(selection)

    assert index_axis
    assert index_axis["class"]
    assert index_axis["domain"]
    assert index_axis["date"]
    assert index_axis["expver"]
    assert index_axis["stream"]
    assert index_axis["time"]

    with pytest.raises(KeyError, match="Couldn't find key:"):
        assert index_axis["non-existing-key"]


def test_fdb_index_axis_in(read_only_fdb_setup):
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

    index_axis: IndexAxis = fdb.axes(selection)

    assert index_axis
    assert len(index_axis.keys()) == 6
    assert "class" in index_axis
    assert "domain" in index_axis
    assert "date" in index_axis
    assert "expver" in index_axis
    assert "stream" in index_axis
    assert "time" in index_axis

    assert "non-existing-key" not in index_axis


def test_index_axis_repr(read_only_fdb_setup):
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

    index_axis: IndexAxis = fdb.axes(selection)

    assert repr(index_axis)


def test_index_axis_keys(read_only_fdb_setup):
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

    index_axis: IndexAxis = fdb.axes(selection)

    assert len(index_axis.keys()) == 6


def test_index_axis_values(read_only_fdb_setup):
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

    index_axis: IndexAxis = fdb.axes(selection)

    assert len(index_axis) == 6


def test_index_axis_items(read_only_fdb_setup):
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

    index_axis: IndexAxis = fdb.axes(selection)

    assert len(index_axis) == 6
    assert len(index_axis.items()) == 6
    assert len(index_axis.keys()) == 6
    assert len(index_axis.values()) == 6

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")


def test_index_axis_items_levels(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        # "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }

    index_axis: IndexAxis = fdb.axes(selection)

    assert len(index_axis) == 11

    print("---------- Level 3: ----------")

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    for k, v in index_axis.items():
        assert index_axis.has_key(k)

    index_axis: IndexAxis = fdb.axes(selection, level=2)

    assert len(index_axis) == 8

    print("---------- Level 2: ----------")

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    index_axis: IndexAxis = fdb.axes(selection, level=1)

    assert len(index_axis) == 6

    print("---------- Level 1: ----------")

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")


def test_index_axis_items_empty_request(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = WildcardMarsSelection()

    index_axis: IndexAxis = fdb.axes(selection)

    assert len(index_axis) == 11

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    for k, v in index_axis.items():
        assert index_axis.has_key(k)
