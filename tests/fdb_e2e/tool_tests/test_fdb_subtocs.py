import datetime

import pytest
from python_api.util.tools import generate_test_files_key_value
from util.run_sh import run_script

subtoc_combinations = [
    pytest.param(
        "simple_fdb_setup_subtocs_no_expver_handler", "simple_env_subtocs_no_expver_handler", marks=pytest.mark.simple
    ),
    pytest.param(
        "simple_fdb_setup_subtocs_expver_handler", "simple_env_subtocs_expver_handler", marks=pytest.mark.simple
    ),
    pytest.param(
        "files_fdb_setup_subtocs_no_expver_handler", "files_env_subtocs_no_expver_handler", marks=pytest.mark.files
    ),
    pytest.param("files_fdb_setup_subtocs_expver_handler", "files_env_subtocs_expver_handler", marks=pytest.mark.files),
]


@pytest.mark.info
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_info_subtoc(fdb_setup, env_setup, function_tmp, info_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

    run_script(script=info_subtoc_script, args=None, cwd=function_tmp, env=env)


@pytest.mark.hide
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_hide(fdb_setup, env_setup, function_tmp, test_data_path, hide_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    source_file_path = test_data_path / "oper.grib"
    generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "0"), ("date", int(yesterday))],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "1"), ("date", int(yesterday))],
        ],
        ["xxxx.0", "xxxx.1"],
    )

    run_script(script=hide_subtoc_script, args=[yesterday], cwd=function_tmp, env=env)


@pytest.mark.grib2fdb
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_grib2fdb(fdb_setup, env_setup, function_tmp, test_data_path, grib2fdb5_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

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
    print(target_files)
    run_script(script=grib2fdb5_subtoc_script, args=None, cwd=function_tmp, env=env)


@pytest.mark.list
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_list(fdb_setup, env_setup, function_tmp, test_data_path, list_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

    source_file_path = test_data_path / "oper.grib"
    target_files = generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [("class", "rd"), ("expver", "xxxx")],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "0")],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "1")],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "2")],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "0")],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "1")],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "2")],
            [("class", "rd"), ("expver", "xxxx")],
            [("class", "rd"), ("expver", "xxxx"), ("date", 20170101)],
            [("class", "rd"), ("expver", "xxxx"), ("date", 20180103)],
        ],
        ["xxxx", "xxxx.0", "xxxx.1", "xxxx.2", "xxxy.0", "xxxy.1", "xxxy.2", "xxxx.d1", "xxxx.d2", "xxxx.d3"],
    )

    assert len(target_files) == 10

    # We are skipping the archive as the tests are expecting an empty FDB
    run_script(script=list_subtoc_script, args=None, cwd=function_tmp, env=env)


@pytest.mark.overlay
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_overlay(fdb_setup, env_setup, function_tmp, test_data_path, overlay_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

    source_file_path = test_data_path / "oper.grib"
    target_files = generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [("class", "rd"), ("expver", "xxxx")],
            [("class", "rd"), ("expver", "xxxy")],
            [("class", "rd"), ("expver", "xxxy"), ("step", "3")],
        ],
        ["xxxx", "xxxy", "xxxy.3"],
    )

    assert len(target_files) == 3

    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    # We are skipping the archive as the tests are expecting an empty FDB
    run_script(script=overlay_subtoc_script, args=[yesterday], cwd=function_tmp, env=env)


@pytest.mark.purge
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_purge(fdb_setup, env_setup, function_tmp, test_data_path, purge_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

    source_file_path = test_data_path / "oper.grib"
    target_files = generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [("class", "rd"), ("expver", "xxxx")],
        ],
        ["xxxx"],
    )

    assert len(target_files) == 1

    # We are skipping the archive as the tests are expecting an empty FDB
    run_script(script=purge_subtoc_script, args=None, cwd=function_tmp, env=env)


@pytest.mark.read
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_read(fdb_setup, env_setup, function_tmp, test_data_path, read_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

    ### Setting up needed quantile data
    source_file_path = test_data_path / "quantile.grib"
    target_files = generate_test_files_key_value(
        source_file_path,
        function_tmp,
        [
            [("class", "rd"), ("expver", "xxxx")],
            [("class", "rd"), ("expver", "xxxy")],
            [("class", "rd"), ("expver", "xxxz")],
        ],
        ["xxxx", "xxxy", "xxxz"],
    )

    quantile_request = (
        "retrieve,"
        "class=rd,"
        "type=cd,"
        "stream=efhs,"
        "expver=xxxy,"
        "levtype=sfc,"
        "param=228,"
        "domain=g,"
        "date=-1,"
        "time=0000,"
        "step=60-132,"
        "quantile=34:100,"
        "target=quantile_request.grb"
    )

    with (function_tmp / "req").open("w+") as req:
        req.write(quantile_request)

    assert len(target_files) == 3

    ### Setting up oper data

    today = (datetime.date.today()).strftime("%Y%m%d")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    before_yesterday = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y%m%d")
    before_before_yesterday = (datetime.date.today() - datetime.timedelta(days=3)).strftime("%Y%m%d")

    source_file_path = test_data_path / "oper.grib"
    target_files.extend(
        generate_test_files_key_value(
            source_file_path,
            function_tmp,
            [
                [("class", "rd"), ("expver", "xxxx"), ("date", int(today))],
                [("class", "rd"), ("expver", "xxxx"), ("date", int(yesterday))],
                [("class", "rd"), ("expver", "xxxx"), ("date", int(before_yesterday))],
                [("class", "rd"), ("expver", "xxxx"), ("date", int(before_before_yesterday))],
                [("class", "rd"), ("expver", "xxxx")],
                [("class", "rd"), ("expver", "xxxy")],
                [("class", "rd"), ("expver", "xxxz")],
            ],
            ["xxxx.0", "xxxx.-1", "xxxx.-2", "xxxx.-3", "source.xxxx", "source.xxxy", "source.xxxz"],
        )
    )

    assert len(target_files) == 10

    for date_diff in [0, -3]:
        request = (
            "retrieve,class=rd,expver=xxxx,type=an,stream=oper,levtype=pl,levelist=1000/850/700/500/400/300,"
            "domain=g,"
            f"date={(datetime.date.today() + datetime.timedelta(days=date_diff)).strftime('%Y%m%d')},"
            "time=0000/1200,"
            "step=0,param=138/155"
        )

        with (function_tmp / f"req.xxxx.{date_diff}").open("w+") as req:
            req.write(request)

    request = (
        "retrieve,class=rd,expver=xxxx,type=an,stream=oper,levtype=pl,levelist=1000/850/700/500/400/300,"
        "domain=g,"
        f"date={before_before_yesterday}/{before_yesterday}/{yesterday}/{today},"
        "time=0000/1200,"
        "step=0,param=138/155"
    )

    with (function_tmp / "req.xxxx.combined").open("w+") as req:
        req.write(request)

    request = (
        "retrieve,class=rd,expver=xxxy,type=an,stream=oper,levtype=pl,levelist=1000/850/700/500/400/300,"
        "domain=g,"
        f"date={yesterday},"
        "time=1200,"
        "step=0,param=138/155"
    )

    with (function_tmp / "req.xxxy.simple").open("w+") as req:
        req.write(request)

    ## Steprange data

    source_file_path = test_data_path / "steprange.grib"
    target_files.extend(
        generate_test_files_key_value(
            source_file_path,
            function_tmp,
            [
                [("class", "rd"), ("expver", "xxxx")],
                [("class", "rd"), ("expver", "xxxy")],
                [("class", "rd"), ("expver", "xxxz")],
            ],
            ["steprange.xxxx", "steprange.xxxy", "steprange.xxxz"],
        )
    )
    request = (
        "retrieve,"
        "class= rd,"
        "type= ep,"
        "stream= enfo,"
        "expver= xxxy,"
        "levtype= sfc,"
        "param= 131070,"
        "domain=g,"
        f"date={yesterday},"
        "time= 1200,"
        "step= 0-24,"
        "target= 'steprange_request.grb'"
    )

    with (function_tmp / "req.xxxy.steprange").open("w+") as req:
        req.write(request)

    # We are skipping the archive as the tests are expecting an empty FDB
    run_script(
        script=read_subtoc_script,
        args=[today, yesterday, before_yesterday, before_before_yesterday],
        cwd=function_tmp,
        env=env,
    )


@pytest.mark.root
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_root(fdb_setup, env_setup, function_tmp, root_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    fdb_request = (
        "class=rd,expver=xxxy,type=an,stream=oper,levtype=pl,levelist=1000/850/700/500/400/300,"
        "domain=g,"
        f"date={yesterday},"
        "time=1200,"
        "step=0,param=138/155"
    )

    with (function_tmp / "req.xxxy.simple").open("w+") as req:
        req.write(fdb_request)

    # We are skipping the archive as the tests are expecting an empty FDB
    run_script(script=root_subtoc_script, args=None, cwd=function_tmp, env=env)


@pytest.mark.wipe
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_wipe(fdb_setup, env_setup, function_tmp, test_data_path, wipe_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

    target_files = []

    source_file_path = test_data_path / "oper.grib"
    target_files.extend(
        generate_test_files_key_value(
            source_file_path,
            function_tmp,
            [
                [("class", "rd"), ("expver", "xxxx")],
                [("class", "rd"), ("expver", "xxxx"), ("type", "an")],
                [("class", "rd"), ("expver", "xxxx"), ("type", "fc")],
                [("class", "rd"), ("expver", "xxxx")],
                [("class", "rd"), ("expver", "xxxx"), ("date", 20170101)],
                [("class", "rd"), ("expver", "xxxx"), ("date", 20180103)],
            ],
            ["xxxx", "xxxx.an", "xxxx.fc", "xxxx.d1", "xxxx.d2", "xxxx.d3"],
        )
    )

    # We are skipping the archive as the tests are expecting an empty FDB
    run_script(script=wipe_subtoc_script, args=None, cwd=function_tmp, env=env)


@pytest.mark.write
@pytest.mark.parametrize("fdb_setup, env_setup", subtoc_combinations)
def test_write(fdb_setup, env_setup, function_tmp, test_data_path, write_subtoc_script, request):
    _ = request.getfixturevalue(fdb_setup)
    env = request.getfixturevalue(env_setup)

    target_files = []

    source_file_path = test_data_path / "oper.grib"
    target_files.extend(
        generate_test_files_key_value(
            source_file_path,
            function_tmp,
            [
                [("class", "rd"), ("expver", "xxxx")],
                [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", 0)],
                [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", 1)],
                [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", 0)],
                [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", 1)],
            ],
            ["xxxx", "xxxx.0", "xxxx.1", "xxxy.0", "xxxy.1"],
        )
    )

    # We are skipping the archive as the tests are expecting an empty FDB
    run_script(script=write_subtoc_script, args=None, cwd=function_tmp, env=env)
