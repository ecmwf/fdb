import datetime

import pytest

from tests.fdb_e2e.tool_tests.util import generate_test_files_key_value, run_script


# ---------------------------------------------------------------------------
# Helper: date strings used across multiple tests
# ---------------------------------------------------------------------------


def _date_offsets() -> tuple[str, str, str, str]:
    """Return (today, yesterday, before_yesterday, before_before_yesterday) as YYYYMMDD."""
    today = datetime.date.today()
    return (
        today.strftime("%Y%m%d"),
        (today - datetime.timedelta(days=1)).strftime("%Y%m%d"),
        (today - datetime.timedelta(days=2)).strftime("%Y%m%d"),
        (today - datetime.timedelta(days=3)).strftime("%Y%m%d"),
    )


# ---------------------------------------------------------------------------
# Tests — one function per FDB operation
#
# fdb_env is parametrised over ALL_LOCAL_SPECS (see fdb_e2e/conftest.py).
# {case}_script is parametrised over both no_subtocs/ and subtocs/ scripts
# (see tool_tests/conftest.py).  The _skip_subtoc_mismatch autouse fixture
# drops combinations where the subtoc mode of the script and the env differ.
# ---------------------------------------------------------------------------


@pytest.mark.info
def test_info(fdb_env, info_script):
    run_script(script=info_script, args=None, cwd=fdb_env.tmp, env=fdb_env.env)


@pytest.mark.hide
def test_hide(fdb_env, test_data_path, hide_script):
    today, yesterday, *_ = _date_offsets()

    generate_test_files_key_value(
        test_data_path / "oper.grib",
        fdb_env.tmp,
        [
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "0"), ("date", int(yesterday))],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "1"), ("date", int(yesterday))],
        ],
        ["xxxx.0", "xxxx.1"],
    )

    run_script(script=hide_script, args=[yesterday], cwd=fdb_env.tmp, env=fdb_env.env)


@pytest.mark.grib2fdb
def test_grib2fdb(fdb_env, test_data_path, grib2fdb5_script):
    target_files = generate_test_files_key_value(
        test_data_path / "oper.grib",
        fdb_env.tmp,
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

    run_script(script=grib2fdb5_script, args=None, cwd=fdb_env.tmp, env=fdb_env.env)


@pytest.mark.list
def test_list(fdb_env, test_data_path, list_script):
    target_files = generate_test_files_key_value(
        test_data_path / "oper.grib",
        fdb_env.tmp,
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

    run_script(script=list_script, args=None, cwd=fdb_env.tmp, env=fdb_env.env)


@pytest.mark.overlay
def test_overlay(fdb_env, test_data_path, overlay_script):
    target_files = generate_test_files_key_value(
        test_data_path / "oper.grib",
        fdb_env.tmp,
        [
            [("class", "rd"), ("expver", "xxxx")],
            [("class", "rd"), ("expver", "xxxy")],
            [("class", "rd"), ("expver", "xxxy"), ("step", "3")],
        ],
        ["xxxx", "xxxy", "xxxy.3"],
    )
    assert len(target_files) == 3

    _, yesterday, *_ = _date_offsets()
    run_script(script=overlay_script, args=[yesterday], cwd=fdb_env.tmp, env=fdb_env.env)


@pytest.mark.purge
def test_purge(fdb_env, test_data_path, purge_script):
    target_files = generate_test_files_key_value(
        test_data_path / "oper.grib",
        fdb_env.tmp,
        [[("class", "rd"), ("expver", "xxxx")]],
        ["xxxx"],
    )
    assert len(target_files) == 1

    run_script(script=purge_script, args=None, cwd=fdb_env.tmp, env=fdb_env.env)


@pytest.mark.read
def test_read(fdb_env, test_data_path, read_script):
    today, yesterday, before_yesterday, before_before_yesterday = _date_offsets()

    # Quantile data
    target_files = generate_test_files_key_value(
        test_data_path / "quantile.grib",
        fdb_env.tmp,
        [
            [("class", "rd"), ("expver", "xxxx")],
            [("class", "rd"), ("expver", "xxxy")],
            [("class", "rd"), ("expver", "xxxz")],
        ],
        ["xxxx", "xxxy", "xxxz"],
    )
    assert len(target_files) == 3

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
    (fdb_env.tmp / "req").write_text(quantile_request)

    # Oper data
    target_files.extend(
        generate_test_files_key_value(
            test_data_path / "oper.grib",
            fdb_env.tmp,
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
        d = (datetime.date.today() + datetime.timedelta(days=date_diff)).strftime("%Y%m%d")
        mars_request = (
            "retrieve,class=rd,expver=xxxx,type=an,stream=oper,levtype=pl,levelist=1000/850/700/500/400/300,"
            f"domain=g,date={d},time=0000/1200,step=0,param=138/155"
        )
        (fdb_env.tmp / f"req.xxxx.{date_diff}").write_text(mars_request)

    mars_request = (
        "retrieve,class=rd,expver=xxxx,type=an,stream=oper,levtype=pl,levelist=1000/850/700/500/400/300,"
        f"domain=g,date={before_before_yesterday}/{before_yesterday}/{yesterday}/{today},"
        "time=0000/1200,step=0,param=138/155"
    )
    (fdb_env.tmp / "req.xxxx.combined").write_text(mars_request)

    mars_request = (
        "retrieve,class=rd,expver=xxxy,type=an,stream=oper,levtype=pl,levelist=1000/850/700/500/400/300,"
        f"domain=g,date={yesterday},time=1200,step=0,param=138/155"
    )
    (fdb_env.tmp / "req.xxxy.simple").write_text(mars_request)

    # Steprange data
    target_files.extend(
        generate_test_files_key_value(
            test_data_path / "steprange.grib",
            fdb_env.tmp,
            [
                [("class", "rd"), ("expver", "xxxx")],
                [("class", "rd"), ("expver", "xxxy")],
                [("class", "rd"), ("expver", "xxxz")],
            ],
            ["steprange.xxxx", "steprange.xxxy", "steprange.xxxz"],
        )
    )
    mars_request = (
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
        "step= 0-24"
    )
    (fdb_env.tmp / "req.xxxy.steprange").write_text(mars_request)

    run_script(
        script=read_script,
        args=[today, yesterday, before_yesterday, before_before_yesterday],
        cwd=fdb_env.tmp,
        env=fdb_env.env,
    )


@pytest.mark.root
def test_root(fdb_env, root_script):
    _, yesterday, *_ = _date_offsets()

    fdb_request = (
        "class=rd,expver=xxxy,type=an,stream=oper,levtype=pl,levelist=1000/850/700/500/400/300,"
        f"domain=g,date={yesterday},time=1200,step=0,param=138/155"
    )
    (fdb_env.tmp / "req.xxxy.simple").write_text(fdb_request)

    run_script(script=root_script, args=None, cwd=fdb_env.tmp, env=fdb_env.env)


@pytest.mark.wipe
def test_wipe(fdb_env, test_data_path, wipe_script):
    generate_test_files_key_value(
        test_data_path / "oper.grib",
        fdb_env.tmp,
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

    run_script(script=wipe_script, args=None, cwd=fdb_env.tmp, env=fdb_env.env)


@pytest.mark.write
def test_write(fdb_env, test_data_path, write_script):
    generate_test_files_key_value(
        test_data_path / "oper.grib",
        fdb_env.tmp,
        [
            [("class", "rd"), ("expver", "xxxx")],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", 0)],
            [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", 1)],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", 0)],
            [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", 1)],
        ],
        ["xxxx", "xxxx.0", "xxxx.1", "xxxy.0", "xxxy.1"],
    )

    run_script(script=write_script, args=None, cwd=fdb_env.tmp, env=fdb_env.env)
