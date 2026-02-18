import pytest
from pyfdb.pyfdb import FDB
from pyfdb.pyfdb_iterator import IndexAxis


def test_index_axis_creation_dict():
    selection = {
        "type": ["an"],
        "class": ["ea"],
        "domain": ["g"],
        "expver": ["0001"],
        "stream": ["oper"],
        "date": ["20200101"],
        "levtype": ["sfc"],
        "step": ["0"],
        "param": ["167", "131", "132"],
        "time": ["1800"],
    }

    index_axis = IndexAxis(selection)

    assert index_axis.items() == selection.items()
    for value in selection.values():
        assert value in index_axis.values()
    assert index_axis.keys() == selection.keys()
    assert index_axis == selection


def test_index_axis_with_dict(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "type": ["an"],
        "class": ["ea"],
        "domain": ["g"],
        "expver": ["0001"],
        "stream": ["oper"],
        "date": ["20200101"],
        "levtype": ["sfc"],
        "step": ["0"],
        "param": ["167", "131", "132"],
        "time": ["1800"],
    }

    index_axis = fdb.axes(selection)

    expected_index_axis = {
        "type": ["an"],
        "class": ["ea"],
        "domain": ["g"],
        "expver": ["0001"],
        "stream": ["oper"],
        "date": ["20200101"],
        "levelist": [""],
        "levtype": ["sfc"],
        "step": ["0"],
        "param": ["131", "132", "167"],
        "time": ["1800"],
    }

    assert index_axis == expected_index_axis
    assert expected_index_axis == index_axis


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
        "param": "131/132/167",
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
        "param": "131/132/167",
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
        "param": "131/132/167",
        "time": "1800",
    }

    index_axis: IndexAxis = fdb.axes(selection)

    assert index_axis
    assert len(index_axis.keys()) == 6
    assert "class" in index_axis
    assert index_axis["class"] == ["ea"]
    assert "domain" in index_axis
    assert index_axis["domain"] == ["g"]
    assert "date" in index_axis
    assert index_axis["date"] == ["20200101"]
    assert "expver" in index_axis
    assert index_axis["expver"] == ["0001"]
    assert "stream" in index_axis
    assert index_axis["stream"] == ["oper"]
    assert "time" in index_axis
    assert index_axis["time"] == ["1800"]

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
        "param": "131/132/167",
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
        "param": "131/132/167",
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
        "param": "131/132/167",
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
        "param": "131/132/167",
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

    index_axis: IndexAxis = fdb.axes({})

    assert len(index_axis) == 11

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    for k, v in index_axis.items():
        assert index_axis.has_key(k)


def test_index_axis_equality(read_only_fdb_setup):
    with FDB(read_only_fdb_setup) as fdb:
        selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "param": "131/132/167",
            "time": "1800",
        }

        index_axis_1: IndexAxis = fdb.axes(selection)
        index_axis_2: IndexAxis = fdb.axes(selection)

        assert index_axis_1 is not index_axis_2
        assert index_axis_1 == index_axis_2

        # Check for inequality if different axis
        index_axis_non_existent: IndexAxis = fdb.axes(
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": "20200101",
                "levtype": "sfc",
                "step": "0",
                "param": "131/132/167",
                "time": "1900",  # This differs
            }
        )

        assert index_axis_1 is not index_axis_non_existent
        assert index_axis_1 != index_axis_non_existent


def test_index_axis_equality_dict(read_only_fdb_setup):
    with FDB(read_only_fdb_setup) as fdb:
        selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "param": "131/132/167",
            "time": "1800",
        }

        index_axis_1: IndexAxis = fdb.axes(selection)

        index_axis_manual = {
            "class": ["ea"],
            "domain": ["g"],
            "expver": "0001",
            "stream": ["oper"],
            "date": ["20200101"],
            "time": ["1800"],
        }

        assert index_axis_1 is not index_axis_manual
        assert index_axis_1 == index_axis_manual

        index_axis_manual_non_existing = {
            "class": ["ea"],
            "domain": ["g"],
            "expver": "0001",
            "stream": ["oper"],
            "date": ["20200101"],
            "time": ["1900"],  # This differs
        }

        assert index_axis_1 is not index_axis_manual_non_existing
        assert index_axis_1 != index_axis_manual_non_existing
