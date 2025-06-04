#!/usr/bin/env python

# (C) Copyright 1996- ECMWF.

# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import shutil

from eccodes import StreamReader

import support.util as util

# Archive
key = {
    "domain": "g",
    "stream": "oper",
    "levtype": "pl",
    "levelist": "300",
    "date": "20191110",
    "time": "0000",
    "step": "0",
    "param": "138",
    "class": "rd",
    "type": "an",
    "expver": "xxxx",
}


def test_archival_read(setup_fdb_tmp_dir, tmp_path_factory, data_path):
    """Full integration test. Testing all of the exposed API of FDB"""
    root_dir, fdb = setup_fdb_tmp_dir()
    tmp_output_directory = tmp_path_factory.mktemp("output")

    print(root_dir)

    with open(data_path / "x138-300.grib", "rb") as filename:
        # This calls the archive function for a None request
        fdb.archive(filename.read())

    key["levelist"] = "400"
    with open(data_path / "x138-400.grib", "rb") as filename:
        fdb.archive(filename.read())

    key["expver"] = "xxxy"
    with open(data_path / "y138-400.grib", "rb") as filename:
        fdb.archive(filename.read())

    fdb.flush()

    # List
    request = {
        "class": "rd",
        "expver": "xxxx",
        "stream": "oper",
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": 0,
        "levelist": [300, "500"],
        "param": ["138", 155, "t"],
    }
    print("direct function, request as dictionary:", request)
    for el in fdb.list(request, True):
        assert el["path"]
        assert el["path"].find("rd:xxxx:oper:20191110:0000:g/an:pl.") != -1
        assert "keys" not in el

    request["levelist"] = ["100", "200", "300", "400", "500", "700", "850", "1000"]
    request["param"] = "138"
    print("")
    print("direct function, updated dictionary:", request)
    it = fdb.list(request, True, True)

    el = next(it)
    assert el["path"]
    assert el["path"].find("rd:xxxx:oper:20191110:0000:g/an:pl.") != -1
    assert el["keys"]
    keys = el["keys"]
    assert keys["class"] == "rd"
    assert keys["levelist"] == "300"

    el = next(it)
    assert el["path"]
    assert el["path"].find("rd:xxxx:oper:20191110:0000:g/an:pl.") != -1
    assert el["keys"]
    keys = el["keys"]
    assert keys["class"] == "rd"
    assert keys["levelist"] == "400"

    try:
        el = next(it)
        assert False, "returned unexpected field"
    except StopIteration:
        assert True, "field listing completed"

    # as an alternative, create a FDB instance and start queries from there
    request["levelist"] = ["400", "500", "700", "850", "1000"]
    print("")
    print("fdb object, request as dictionary:", request)
    for el in fdb.list(request, True, True):
        assert el["path"]
        assert el["path"].find("rd:xxxx:oper:20191110:0000:g/an:pl.") != -1
        assert el["keys"]
        keys = el["keys"]
        assert keys["class"] == "rd"
        assert keys["levelist"] == "400"

    # Retrieve
    request = {
        "domain": "g",
        "stream": "oper",
        "levtype": "pl",
        "step": "0",
        "expver": "xxxx",
        "date": "20191110",
        "class": "rd",
        "levelist": "300",
        "param": "138",
        "time": "0000",
        "type": "an",
    }

    # x138-300
    print("")
    with (
        open(tmp_output_directory / "x138-300bis.grib", "wb") as o,
        fdb.retrieve(request) as i,
    ):
        print("save to file ", tmp_output_directory / "x138-300bis.grib")
        shutil.copyfileobj(i, o)

    assert util.check_grib_files_for_same_content(
        data_path / "x138-300.grib",
        tmp_output_directory / "x138-300bis.grib",
    )

    # x138-400
    request["levelist"] = "400"
    with (
        open(tmp_output_directory / "x138-400bis.grib", "wb") as o,
        fdb.retrieve(request) as i,
    ):
        print("save to file ", tmp_output_directory / "x138-400bis.grib")
        shutil.copyfileobj(i, o)

    assert util.check_grib_files_for_same_content(
        data_path / "x138-400.grib",
        tmp_output_directory / "x138-400bis.grib",
    )

    # y138-400
    request["expver"] = "xxxy"
    with (
        open(tmp_output_directory / "y138-400bis.grib", "wb") as o,
        fdb.retrieve(request) as i,
    ):
        print("save to file ", tmp_output_directory / "y138-400bis.grib")
        shutil.copyfileobj(i, o)

    assert util.check_grib_files_for_same_content(
        data_path / "y138-400.grib",
        tmp_output_directory / "y138-400bis.grib",
    )

    print("")
    print("FDB retrieve")
    print("direct function, retrieve from request:", request)
    datareader = fdb.retrieve(request)

    print("")
    print("reading a small chunk")
    chunk = datareader.read(10)
    print(chunk)
    print("tell()", datareader.tell())

    print("go back (partially) - seek(2)")
    datareader.seek(2)
    print("tell()", datareader.tell())

    print("reading a larger chunk")
    chunk = datareader.read(40)
    print(chunk)

    print("go back - seek(0)")
    datareader.seek(0)

    print("")
    print("decode GRIB")

    reader = StreamReader(datareader)
    next(reader)

    request["levelist"] = [300, "400"]
    request["expver"] = "xxxx"

    print("")
    with (
        open(tmp_output_directory / "foo.grib", "wb") as o,
        fdb.retrieve(request) as i,
    ):
        print("save to file ", tmp_output_directory / "foo.grib")
        shutil.copyfileobj(i, o)

    # Check whether retrieval of two field is equal with the
    # data content of the individual fields
    with open(tmp_output_directory / "foo.grib", "rb") as merged_file:
        merged_reader = StreamReader(merged_file)
        merged_grib = next(merged_reader)

        with open(data_path / "x138-300.grib", "rb") as grib_part_1:
            reader1 = StreamReader(grib_part_1)
            grib_part_1 = next(reader1)

            assert util.check_numpy_array_equal(merged_grib.data, grib_part_1.data)

        merged_grib = next(merged_reader)

        with open(data_path / "x138-400.grib", "rb") as grib_part_2:
            reader2 = StreamReader(grib_part_2)
            grib_part_2 = next(reader2)

            assert util.check_numpy_array_equal(merged_grib.data, grib_part_2.data)
