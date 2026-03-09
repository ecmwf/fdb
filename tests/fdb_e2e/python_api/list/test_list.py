import datetime
from pathlib import Path

import pytest
from python_api.util.tools import Tool

from pyfdb import FDB
from pyfdb.pyfdb_iterator import ListElement


def generate_test_files_expver_step(source_file_path, function_tmp):
    # Data setup
    target_files: list[Path] = []

    for step in range(0, 3):
        for expver in ["xxxx", "xxxy"]:
            target_file_path = function_tmp / f"data.{expver}.{step}.grib"
            Tool.modify_metadata_source_file(
                [("class", "rd"), ("expver", expver), ("type", "fc"), ("step", str(step))],
                source_file=source_file_path,
                target_file=target_file_path,
            )
            target_files.append(target_file_path)

    generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "0")],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "0")],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "1")],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "1")],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "2")],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "2")],
        ],
        [
            "data.xxxx.0.grib",
            "data.xxxy.0.grib",
            "data.xxxx.1.grib",
            "data.xxxy.1.grib",
            "data.xxxx.2.grib",
            "data.xxxy.2.grib",
        ],
    )

    return target_files


def generate_test_files_key_value(
    source_file_path,
    function_tmp,
    key_value_list: list[list[tuple[str, str | int]]],
    output_file_names: list[str],
):
    """Creates copies of the GRIB file at source_file_path in function_tmp. For each
       element of the key_value_list all given key-value-pairs are modified in the resulting GRIB
       file and the corresponding entry from output_file_names is used as a name.

       This means:

       setup_fdb_key_value(
           source_file_path,
           function_tmp,
           [
               [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "0")],
               [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "0")],
               [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "1")],
               [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "1")],
               [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "2")],
               [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "2")],
           ],
           [
               "data.xxxx.0.grib",
               "data.xxxy.0.grib",
               "data.xxxx.1.grib",
               "data.xxxy.1.grib",
               "data.xxxx.2.grib",
               "data.xxxy.2.grib",
           ],
       )

        Is creating 6 files with the names of output_file_names. For each file the GRIB metadata
        is modified as given in key_value_list.


    Args:
        source_file_path (Path): Path to the template GRIB file
        function_tmp (Path): Path to the output folder
        key_value_list: List of key-value pair lists for modifying the GRIB metadata
        output_file_names: Names of the output files

    Returns:
        target_files - A list of paths to the created files
    """
    assert len(key_value_list) == len(output_file_names)

    # Data setup
    target_files: list[Path] = []

    for i in range(len(key_value_list)):
        cur_key_value_list = key_value_list[i]
        output_file_name = output_file_names[i]
        target_file_path = function_tmp / f"data.{output_file_name}.grib"
        Tool.modify_metadata_source_file(
            cur_key_value_list,
            source_file=source_file_path,
            target_file=target_file_path,
        )
        target_files.append(target_file_path)

    return target_files


@pytest.mark.parametrize("no_files_saved", [1, 2, 3, 4, 5, 6])
def test_list_all(simple_fdb_setup, data_path, function_tmp, no_files_saved):
    # Data setup
    source_file_path = data_path / "oper.grib"

    target_files: list[Path] = []

    for step in range(0, 3):
        for expver in ["xxxx", "xxxy"]:
            target_file_path = function_tmp / f"data.{expver}.{step}.grib"
            Tool.modify_metadata_source_file(
                [("class", "rd"), ("expver", expver), ("type", "fc"), ("step", str(step))],
                source_file=source_file_path,
                target_file=target_file_path,
            )
            target_files.append(target_file_path)

    with FDB(simple_fdb_setup) as fdb:
        for i in range(no_files_saved):
            fdb.archive(target_files[i].read_bytes())

    list_elements = list(fdb.list({}))
    assert len(list_elements) == 24 * no_files_saved

    list_elements = list(fdb.list({"class": "rd", "expver": "xxxx", "step": "0"}))
    assert len(list_elements) == 24

    list_elements = list(fdb.list({"class": "rd", "expver": "xxxy", "step": "0"}))
    assert len(list_elements) == (24 if no_files_saved >= 2 else 0)

    list_elements = list(fdb.list({"class": "rd", "expver": "xxxx", "step": "1"}))
    assert len(list_elements) == (24 if no_files_saved >= 3 else 0)

    list_elements = list(fdb.list({"class": "rd", "expver": "xxxy", "step": "1"}))
    assert len(list_elements) == (24 if no_files_saved >= 4 else 0)

    list_elements = list(fdb.list({"class": "rd", "expver": "xxxx", "step": "2"}))
    assert len(list_elements) == (24 if no_files_saved >= 5 else 0)

    list_elements = list(fdb.list({"class": "rd", "expver": "xxxy", "step": "2"}))
    assert len(list_elements) == (24 if no_files_saved >= 6 else 0)


def test_list_all_including_masked(simple_fdb_setup, data_path, function_tmp):
    fdb = FDB(simple_fdb_setup)

    source_file_path = data_path / "oper.grib"
    target_files = generate_test_files_expver_step(source_file_path, function_tmp)

    # Second run masks the elements
    for i in range(2):
        with fdb:
            for target_file in target_files:
                fdb.archive(target_file.read_bytes())

        # Check total amount of elements in the FDB
        list_elements = list(fdb.list({}, include_masked=True))
        assert len(list_elements) == 144 * (i + 1)

        for expver in ["xxxx", "xxxy"]:
            for step in range(0, 3):
                # Check that for every of the given test files there are exact 24 entries
                # In the second loop, those are masked and should return twice as many elements
                list_elements = list(fdb.list({"class": "rd", "expver": expver, "step": step}, include_masked=True))
                assert len(list_elements) == 24 * (i + 1)

                # Also check that without the masked elements we only retain 24 elements
                list_elements = list(fdb.list({"class": "rd", "expver": expver, "step": step}))
                assert len(list_elements) == 24


def test_list_location(simple_fdb_setup, data_path, function_tmp):
    fdb = FDB(simple_fdb_setup)

    source_file_path = data_path / "oper.grib"
    target_files = generate_test_files_expver_step(source_file_path, function_tmp)

    # Second run masks the elements
    with fdb:
        fdb.archive(target_files[0].read_bytes())

    # Check total amount of elements in the FDB
    list_elements = list(fdb.list({}))
    assert len(list_elements) == 24

    # Check total amount of elements at time 0000
    list_elements: list[ListElement] = list(fdb.list({"class": "rd", "expver": "xxxx", "time": "0000"}))

    locations = []

    for el in list_elements:
        print(f"URI: {el.uri}, Offset: {el.offset()}, Length: {el.length()}")
        assert el.uri
        locations.append(el.uri)

    assert len(locations) == 12, "Wrong amount of archived data for expver 'xxxx' and time '0000' should be 12"

    for el in list_elements:
        print(f"{el}")


def test_list_masking(simple_fdb_setup, data_path, function_tmp):
    fdb = FDB(simple_fdb_setup)

    yesterday = f"{(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')}"

    source_file_path = data_path / "oper.grib"
    target_files = generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("date", int(yesterday)),
            ],
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("date", 20170101),
            ],
        ],
        ["xxxx.d1", "xxxx.d2"],
    )

    for _ in range(2):
        with fdb:
            for target_file in target_files:
                fdb.archive(target_file.read_bytes())

    # Check total amount of elements in the FDB
    list_elements = list(fdb.list({}))
    assert len(list_elements) == 48

    # Check total amount of elements in the FDB with masked included
    list_elements = list(fdb.list({}, include_masked=True))
    assert len(list_elements) == 96

    # Check total amount of elements in the FDB with masked included
    selection = {
        "class": "rd",
        "expver": "xxxx",
        "date": ["-1", "20170101"],
        "stream": "oper",
        "type": "an",
        "levtype": "pl",
        "param": ["155", "138"],
        "levelist": ["300", "400", "500", "700", "850", "1000"],
    }
    list_elements = list(fdb.list(selection))
    assert len(list_elements) == 48, "Entries should be masked per default"

    list_elements = list(fdb.list(selection, include_masked=True))
    assert len(list_elements) == 96, "Masked entries should be shown"

    selection_non_existing_param_level = {
        "class": "rd",
        "expver": "xxxx",
        "date": ["-1", "20170101"],
        "stream": "oper",
        "type": "an",
        "levtype": "pl",
        "param": ["130", "138"],
        "levelist": ["300", "123", "1000"],
    }

    list_elements = list(fdb.list(selection_non_existing_param_level))
    assert len(list_elements) == 8, "Entries should be masked per default"

    list_elements = list(fdb.list(selection_non_existing_param_level, include_masked=True))
    assert len(list_elements) == 16, "Masked entries should be shown"


# TODO(TKR): Should we include a test case for the minimum keys or is this not necessary due to the tools not being covered


def test_list_ranges_include_masked(simple_fdb_setup, data_path, function_tmp):
    fdb = FDB(simple_fdb_setup)

    yesterday = f"{(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')}"

    source_file_path = data_path / "oper.grib"
    target_files = generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("date", int(yesterday)),
            ],
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("date", 20170101),
            ],
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("date", 20180101),
            ],
        ],
        ["xxxx.d1", "xxxx.d2", "xxxx.d3"],
    )

    with fdb:
        for target_file in target_files:
            fdb.archive(target_file.read_bytes())

    all_entries = list(fdb.list({}, include_masked=True))
    assert len(all_entries) == 72

    all_entries = list(fdb.list({}))
    assert len(all_entries) == 72


request_entries_tuples = [
    ({"class": "rd", "expver": "xxxx", "date": "-1"}, 24),
    ({"class": "rd", "expver": "xxxx", "date": "-1/20170101"}, 48),
    ({"class": "rd", "expver": "xxxx", "date": ["-1", "20170101"]}, 48),
    ({"class": "rd", "expver": "xxxx", "date": "-1/20170101/20180101"}, 72),
    ({"class": "rd", "expver": "xxxx", "date": ["-1", "20170101", "20180101"]}, 72),
    (
        {
            "class": "rd",
            "expver": "xxxx",
            "date": ["-1", "20170101", "20180101"],
            "stream": "oper",
            "type": "an",
            "levtype": "pl",
            "param": 60,
        },
        0,
    ),
    (
        {
            "class": "rd",
            "expver": "xxxx",
            "date": "20060101",
            "stream": "oper",
            "type": "an",
            "levtype": "pl",
            "param": 155,
        },
        0,
    ),
    (
        {
            "class": "rd",
            "expver": "xxxx",
            "date": ["-1", "20170101", "20180101", "20060101"],
            "stream": "oper",
            "type": "an",
            "levtype": "pl",
            "param": 155,
        },
        36,
    ),
    (
        {
            "class": "rd",
            "expver": "xxxx",
            "date": ["-1", "20170101", "20180101", "20060101"],  # date 20060101 is not available
            "stream": "oper",
            "type": "an",
            "levtype": "pl",
            "param": [155, 138],
        },
        72,
    ),
    (
        {
            "class": "rd",
            "expver": "xxxx",
            "date": ["-1", "20170101", "20180101", "20060101"],  # date 20060101 is not available
            "stream": "oper",
            "type": "an",
            "levtype": "pl",
            "param": [155, 60, 138],  # param 60 not available
        },
        72,
    ),
    (
        {
            "class": "rd",
            "expver": "xxxx",
            "date": ["-1", "20170101", "20180101", "20060101"],  # date 20060101 is not available
            "stream": "oper",
            "type": "an",
            "levtype": "pl",
            "levelist": "300/400/500/700/850/1000",
            "param": [155, 60, 138],  # param 60 not available
        },
        72,
    ),
    (
        {
            "class": "rd",
            "expver": "xxxx",
            "date": ["-1", "20170101", "20180101", "20060101"],  # date 20060101 is not available
            "stream": "oper",
            "type": "an",
            "levtype": "pl",
            "levelist": "300/123/1000",
            "param": [155, 60, 138],  # param 60 not available
        },
        24,
    ),
]


@pytest.mark.parametrize("selection, expected_entries", request_entries_tuples)
def test_list_ranges(simple_fdb_setup, data_path, function_tmp, selection, expected_entries):
    fdb = FDB(simple_fdb_setup)

    yesterday = f"{(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')}"

    source_file_path = data_path / "oper.grib"
    target_files = generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("date", int(yesterday)),
            ],
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("date", 20170101),
            ],
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("date", 20180101),
            ],
        ],
        ["xxxx.d1", "xxxx.d2", "xxxx.d3"],
    )

    with fdb:
        for target_file in target_files:
            fdb.archive(target_file.read_bytes())

    all_entries = list(fdb.list(selection))
    assert len(all_entries) == expected_entries
