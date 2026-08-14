# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import importlib
import sys
import warnings
from unittest.mock import MagicMock

import pytest

import pyfdb._internal as _internal
from pyfdb._internal import _check_fdb5_version_compatibility

BUILD_VERSION = "5.22.1"
MISMATCHED_VERSION = "5.99.0"

MATCHING_RUNTIME_INFO = [
    ("fdb", BUILD_VERSION, "abc1234", "/fake/fdb/lib/libfdb5.so"),
    ("eckit", "1.32.5", "def5678", "/fake/eckit/lib/libeckit.so"),
]

MISMATCHED_RUNTIME_INFO = [
    ("fdb", MISMATCHED_VERSION, "abc1234", "/fake/fdb/lib/libfdb5.so"),
    ("eckit", "1.32.5", "def5678", "/fake/eckit/lib/libeckit.so"),
]

NO_FDB_RUNTIME_INFO = [
    ("eckit", "1.32.5", "def5678", "/fake/eckit/lib/libeckit.so"),
]


# ---------------------------------------------------------------------------
# _check_fdb5_version_compatibility — direct function tests
# ---------------------------------------------------------------------------


def test_no_warning_when_versions_match():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _check_fdb5_version_compatibility(BUILD_VERSION, MATCHING_RUNTIME_INFO)
    assert not w


def test_warning_on_version_mismatch():
    with pytest.warns(UserWarning):
        _check_fdb5_version_compatibility(BUILD_VERSION, MISMATCHED_RUNTIME_INFO)


def test_warning_is_user_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _check_fdb5_version_compatibility(BUILD_VERSION, MISMATCHED_RUNTIME_INFO)
    assert len(w) == 1
    assert issubclass(w[0].category, UserWarning)


def test_warning_message_contains_build_version():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _check_fdb5_version_compatibility(BUILD_VERSION, MISMATCHED_RUNTIME_INFO)
    assert BUILD_VERSION in str(w[0].message)


def test_warning_message_contains_runtime_version():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _check_fdb5_version_compatibility(BUILD_VERSION, MISMATCHED_RUNTIME_INFO)
    assert MISMATCHED_VERSION in str(w[0].message)


def test_only_one_warning_emitted():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _check_fdb5_version_compatibility(BUILD_VERSION, MISMATCHED_RUNTIME_INFO)
    assert len(w) == 1


def test_warning_message_contains_deps_hint():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _check_fdb5_version_compatibility(BUILD_VERSION, MISMATCHED_RUNTIME_INFO)
    assert "--print-home-deps" in str(w[0].message)


def test_no_extra_warnings_on_match():
    """Matching versions must produce exactly zero warnings."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _check_fdb5_version_compatibility(BUILD_VERSION, MATCHING_RUNTIME_INFO)
    version_warnings = [x for x in w if issubclass(x.category, UserWarning)]
    assert not version_warnings


def test_raises_when_fdb_absent_from_runtime_info():
    """If libfdb5 did not register itself the library likely failed to load."""
    with pytest.raises(RuntimeError):
        _check_fdb5_version_compatibility(BUILD_VERSION, NO_FDB_RUNTIME_INFO)


def test_raises_on_empty_runtime_info():
    with pytest.raises(RuntimeError):
        _check_fdb5_version_compatibility(BUILD_VERSION, [])


def test_exception_message_contains_deps_hint():
    with pytest.raises(RuntimeError, match="--print-home-deps"):
        _check_fdb5_version_compatibility(BUILD_VERSION, NO_FDB_RUNTIME_INFO)


# ---------------------------------------------------------------------------
# Import-time behaviour — tests via module reload
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_bindings(monkeypatch):
    """Replace pyfdb_bindings.pyfdb_bindings in sys.modules with a MagicMock
    so that importlib.reload(pyfdb._internal) picks up controlled values.

    Teardown restores sys.modules *before* reloading pyfdb._internal so that
    the cleanup reload binds _internal's names to the real module, not the
    mock. Relying on monkeypatch to restore sys.modules would be too late
    (monkeypatch tears down after this fixture), leaving _internal holding
    mock references that leak into subsequent tests."""
    real_mod = sys.modules.get("pyfdb_bindings.pyfdb_bindings")
    mock_mod = MagicMock(spec=real_mod)
    # spec copies attribute names but not values; set the ones we need explicitly
    mock_mod.version_info = MagicMock(return_value=[])
    mock_mod.__fdb5_build_version__ = BUILD_VERSION
    monkeypatch.setitem(sys.modules, "pyfdb_bindings.pyfdb_bindings", mock_mod)
    yield mock_mod
    # Restore the real module first so the reload below picks up real bindings.
    sys.modules["pyfdb_bindings.pyfdb_bindings"] = real_mod
    importlib.reload(_internal)


def test_import_time_warning_on_mismatch(mock_bindings):
    mock_bindings.version_info.return_value = MISMATCHED_RUNTIME_INFO

    with pytest.warns(UserWarning, match=BUILD_VERSION):
        importlib.reload(_internal)


def test_import_time_warning_message_contains_runtime_version(mock_bindings):
    mock_bindings.version_info.return_value = MISMATCHED_RUNTIME_INFO

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        importlib.reload(_internal)

    user_warnings = [x for x in w if issubclass(x.category, UserWarning)]
    assert len(user_warnings) == 1
    assert MISMATCHED_VERSION in str(user_warnings[0].message)


def test_import_time_no_warning_on_matching_versions(mock_bindings):
    mock_bindings.version_info.return_value = MATCHING_RUNTIME_INFO

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        importlib.reload(_internal)

    version_warnings = [x for x in w if issubclass(x.category, UserWarning)]
    assert not version_warnings
