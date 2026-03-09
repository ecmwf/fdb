import datetime
import os

from python_api.util.tools import generate_test_files_key_value
from util.run_sh import run_script

from pyfdb.pyfdb import FDB


def test_info(simple_fdb_setup, function_tmp, info_script):
    env = {}
    env["PATH"] = "/Users/tkremer/Code/stack_2/build/bin" + os.pathsep + os.getenv("PATH", "")

    fdb = FDB(simple_fdb_setup)

    # Check that the temporary fdb is configured and picked up
    assert str(function_tmp) in fdb.config()[0]["schema"]
    all(str(function_tmp) in root["path"] for root in fdb.config()[0]["spaces"][0]["roots"])

    run_script(script=info_script, args=None, cwd=function_tmp, env=env)


def test_hide(simple_fdb_setup, function_tmp, test_data_path, hide_script):
    env = {}
    env["PATH"] = "/Users/tkremer/Code/stack_2/build/bin" + os.pathsep + os.getenv("PATH", "")
    env["FDB_HOME"] = str(function_tmp)

    fdb = FDB(simple_fdb_setup)
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    # Check that the temporary fdb is configured and picked up
    assert str(function_tmp) in fdb.config()[0]["schema"]
    all(str(function_tmp) in root["path"] for root in fdb.config()[0]["spaces"][0]["roots"])

    source_file_path = test_data_path / "oper.grib"
    target_files = generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("type", "fc"),
                ("step", "0"),
                ("date", int(yesterday)),
            ],
            [
                ("class", "rd"),
                ("expver", "xxxx"),
                ("type", "fc"),
                ("step", "1"),
                ("date", int(yesterday)),
            ],
        ],
        ["xxxx.0", "xxxx.1"],
    )

    with fdb:
        for target_file in target_files:
            fdb.archive(target_file.read_bytes())

    run_script(script=hide_script, args=[yesterday], cwd=function_tmp, env=env)


def test_grib2fdb5(simple_fdb_setup, function_tmp, test_data_path, grib2fdb5_script):
    # TODO(TKR): In the check.sh was a switch, which altered the result depending on whether
    # you have subtocs enabled or not. This needs to be respected for the non-simple-case
    # Same for ignored cases, same for simple
    env = {}
    env["PATH"] = "/Users/tkremer/Code/stack_2/build/bin" + os.pathsep + os.getenv("PATH", "")
    env["FDB_HOME"] = str(function_tmp)

    fdb = FDB(simple_fdb_setup)

    # Check that the temporary FDB is configured and picked up
    assert str(function_tmp) in fdb.config()[0]["schema"]
    all(str(function_tmp) in root["path"] for root in fdb.config()[0]["spaces"][0]["roots"])

    source_file_path = test_data_path / "oper.grib"
    target_files = generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [("class", "rd"), ("expver", "xxxx")],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "0")],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "1")],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "0")],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "1")],
        ],
        ["xxxx", "xxxx.0", "xxxx.1", "xxxy.0", "xxxy.1"],
    )

    assert len(target_files) == 5

    # We are skipping the archive as the tests are expecting an empty FDB

    run_script(script=grib2fdb5_script, args=None, cwd=function_tmp, env=env)
