# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import findlibs
import pytest

from pyfdb.__main__ import DEPENDENCY_ORDER, OPTIONAL_DEPENDENCIES, main


ALL_LIBS = {
    "fdb5": "/fake/fdb/lib/libfdb5.so",
    "eckit": "/fake/eckit/lib/libeckit.so",
    "metkit": "/fake/metkit/lib/libmetkit.so",
    "eccodes": "/fake/eccodes/lib/libeccodes.so",
}


def _run_cli(args, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["pyfdb"] + args)
    try:
        main()
        return 0
    except SystemExit as exc:
        return exc.code


def _expected_entry(name, path):
    """Build the expected log fragment for a dependency line."""
    label = f"{name} [Optional]" if name in OPTIONAL_DEPENDENCIES else name
    home = Path(path).parent.parent
    return f"{label}: {home}"


@pytest.fixture(autouse=True)
def capture_info_logs(caplog):
    caplog.set_level(logging.INFO)


@pytest.fixture
def find_mock(monkeypatch):
    mock = MagicMock(side_effect=lambda name: ALL_LIBS.get(name))
    monkeypatch.setattr(findlibs, "find", mock)
    return mock


# ---------------------------------------------------------------------------
# --print-home
# ---------------------------------------------------------------------------


def test_print_home_success(find_mock, monkeypatch, caplog):
    exit_code = _run_cli(["--print-home"], monkeypatch)
    assert exit_code == 0
    assert "/fake/fdb" in caplog.text


def test_print_home_strips_lib_component(monkeypatch, caplog):
    monkeypatch.setattr(
        findlibs, "find", MagicMock(return_value="/opt/ecmwf/lib64/libfdb5.so")
    )
    exit_code = _run_cli(["--print-home"], monkeypatch)
    assert exit_code == 0
    assert "/opt/ecmwf" in caplog.text
    assert "lib64" not in caplog.text


def test_print_home_not_found(monkeypatch, caplog):
    monkeypatch.setattr(findlibs, "find", MagicMock(return_value=None))
    exit_code = _run_cli(["--print-home"], monkeypatch)
    assert exit_code == 1
    assert "not found" in caplog.text


def test_print_home_calls_find_with_fdb5(find_mock, monkeypatch):
    _run_cli(["--print-home"], monkeypatch)
    find_mock.assert_called_once_with("fdb5")


# ---------------------------------------------------------------------------
# --print-home-deps
# ---------------------------------------------------------------------------


def test_print_home_deps_all_found(find_mock, monkeypatch, caplog):
    exit_code = _run_cli(["--print-home-deps"], monkeypatch)
    assert exit_code == 0
    for name, path in ALL_LIBS.items():
        assert _expected_entry(name, path) in caplog.text


def test_print_home_deps_missing_required_exits_nonzero(monkeypatch, caplog):
    libs_without_metkit = {k: v for k, v in ALL_LIBS.items() if k != "metkit"}
    monkeypatch.setattr(
        findlibs, "find", MagicMock(side_effect=lambda n: libs_without_metkit.get(n))
    )
    exit_code = _run_cli(["--print-home-deps"], monkeypatch)
    assert exit_code == 1
    assert "metkit" in caplog.text


def test_print_home_deps_missing_required_logs_error(monkeypatch, caplog):
    libs_without_metkit = {k: v for k, v in ALL_LIBS.items() if k != "metkit"}
    monkeypatch.setattr(
        findlibs, "find", MagicMock(side_effect=lambda n: libs_without_metkit.get(n))
    )
    _run_cli(["--print-home-deps"], monkeypatch)
    errors = [
        r
        for r in caplog.records
        if r.levelno == logging.ERROR and "metkit" in r.message
    ]
    assert errors


def test_print_home_deps_missing_optional_exits_nonzero(monkeypatch):
    libs_without_eccodes = {k: v for k, v in ALL_LIBS.items() if k != "eccodes"}
    monkeypatch.setattr(
        findlibs, "find", MagicMock(side_effect=lambda n: libs_without_eccodes.get(n))
    )
    exit_code = _run_cli(["--print-home-deps"], monkeypatch)
    assert exit_code == 1


def test_print_home_deps_missing_optional_logs_info_not_error(monkeypatch, caplog):
    libs_without_eccodes = {k: v for k, v in ALL_LIBS.items() if k != "eccodes"}
    monkeypatch.setattr(
        findlibs, "find", MagicMock(side_effect=lambda n: libs_without_eccodes.get(n))
    )
    _run_cli(["--print-home-deps"], monkeypatch)
    eccodes_records = [r for r in caplog.records if "eccodes" in r.message]
    assert eccodes_records
    assert all(r.levelno == logging.INFO for r in eccodes_records)


def test_print_home_deps_optional_marker_in_message(monkeypatch, caplog):
    libs_without_eccodes = {k: v for k, v in ALL_LIBS.items() if k != "eccodes"}
    monkeypatch.setattr(
        findlibs, "find", MagicMock(side_effect=lambda n: libs_without_eccodes.get(n))
    )
    _run_cli(["--print-home-deps"], monkeypatch)
    assert "[Optional]" in caplog.text


def test_print_home_deps_queries_all_deps(find_mock, monkeypatch):
    _run_cli(["--print-home-deps"], monkeypatch)
    queried = {call.args[0] for call in find_mock.call_args_list}
    assert queried == set(DEPENDENCY_ORDER)


def test_print_home_deps_disable_vars_appear_before_homes(
    find_mock, monkeypatch, caplog
):
    monkeypatch.setenv("FINDLIBS_DISABLE_FDB5", "1")
    _run_cli(["--print-home-deps"], monkeypatch)
    lines = caplog.text.splitlines()
    disable_idx = next(
        i for i, line in enumerate(lines) if "FINDLIBS_DISABLE_FDB5" in line
    )
    first_dep_idx = next(i for i, line in enumerate(lines) if "fdb5:" in line)
    assert disable_idx < first_dep_idx


# ---------------------------------------------------------------------------
# Output format
# ---------------------------------------------------------------------------


def test_logging_format(monkeypatch):
    calls = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(
        findlibs, "find", MagicMock(return_value="/fake/fdb/lib/libfdb5.so")
    )
    _run_cli(["--print-home"], monkeypatch)
    assert calls, "basicConfig must be called"
    fmt = calls[0]["format"]
    assert "%(asctime)s" in fmt
    assert "%(levelname)" in fmt
    assert "%(message)s" in fmt


def test_verbose_sets_debug_level(monkeypatch):
    calls = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(
        findlibs, "find", MagicMock(return_value="/fake/fdb/lib/libfdb5.so")
    )
    _run_cli(["--print-home", "--verbose"], monkeypatch)
    assert calls[0]["level"] == logging.DEBUG


def test_default_level_is_info(monkeypatch):
    calls = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(
        findlibs, "find", MagicMock(return_value="/fake/fdb/lib/libfdb5.so")
    )
    _run_cli(["--print-home"], monkeypatch)
    assert calls[0]["level"] == logging.INFO


# ---------------------------------------------------------------------------
# No arguments
# ---------------------------------------------------------------------------


def test_no_args_exits_with_code_2(monkeypatch):
    exit_code = _run_cli([], monkeypatch)
    assert exit_code == 2
