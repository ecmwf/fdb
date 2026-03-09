import datetime
import os

import pytest

from pyfdb.pyfdb import FDB
from python_api.util.tools import generate_test_files_key_value
from util.run_sh import run_script


def test_info(simple_fdb_setup, function_tmp, info_script):
    env = {}
    env["PATH"] = "/Users/tkremer/Code/stack_2/build/bin" + os.pathsep + os.getenv("PATH", "")

    run_script(script=info_script, args=None, cwd=function_tmp, env=env)


def test_hide(simple_fdb_setup, function_tmp, test_data_path, hide_script):
    env = {}
    env["PATH"] = "/Users/tkremer/Code/stack_2/build/bin" + os.pathsep + os.getenv("PATH", "")
    env["FDB_HOME"] = str(function_tmp)

    fdb = FDB(simple_fdb_setup)
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

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
