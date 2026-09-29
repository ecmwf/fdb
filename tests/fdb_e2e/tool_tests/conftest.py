import pathlib
from pathlib import Path

import git
import pytest

from fdb_e2e.conftest import ALL_LOCAL_SPECS


def _get_git_root(path) -> Path:
    git_repo = git.Repo(path, search_parent_directories=True)
    return Path(git_repo.git.rev_parse("--show-toplevel"))


def _tool_tests_dir() -> Path:
    return _get_git_root(__file__) / "tests" / "fdb_e2e" / "tool_tests"


def search_scripts(case: str, subtocs: bool) -> list[Path]:
    """Return sorted shell scripts for *case* in the no_subtocs or subtocs directory."""
    subdir = "subtocs" if subtocs else "no_subtocs"
    return sorted((_tool_tests_dir() / subdir / case).glob("*.sh"))


@pytest.fixture(scope="function")
def test_data_path() -> pathlib.Path:
    """Provides path to test data (resolved via git root)."""
    git_repo = git.Repo(__file__, search_parent_directories=True)
    git_root = pathlib.Path(git_repo.git.rev_parse("--show-toplevel"))
    path = git_root / "tests" / "fdb_e2e" / "data"
    assert path.exists()
    return path


# Cases whose scripts live under no_subtocs/<case>/ and subtocs/<case>/.
_CASES = ["info", "hide", "grib2fdb5", "list", "purge", "read", "root", "wipe", "write"]


def _make_script_params(case: str) -> list:
    """
    Build pytest.param entries pairing each FdbEnvSpec with the scripts that
    match its subtoc mode.  Only valid (spec, script) combinations are
    generated — no cartesian product, no runtime skips.

    ID format: ``{spec.id}::{script.name}``  e.g. ``simple::simple.sh``.
    The ``::`` separator makes the FDB-env portion visually distinct from the
    shell script name in pytest -v output and --collect-only listings.
    """
    params = []
    for spec in ALL_LOCAL_SPECS:
        for script in search_scripts(case, subtocs=spec.subtocs):
            params.append(
                pytest.param(
                    spec,
                    script,
                    id=f"{spec.id}::{script.name}",
                    marks=spec.marks,
                )
            )
    return params


def pytest_generate_tests(metafunc):
    for case in _CASES:
        script_param = f"{case}_script"
        if script_param not in metafunc.fixturenames:
            continue

        params = _make_script_params(case)
        # fdb_env is indirect: pytest passes the FdbEnvSpec as request.param
        metafunc.parametrize(["fdb_env", script_param], params, indirect=["fdb_env"])

    # Overlay is handled separately: wipe.sh is xfail pending FDB-652.
    if "overlay_script" in metafunc.fixturenames:
        params = []
        for spec in ALL_LOCAL_SPECS:
            for script in search_scripts("overlay", subtocs=spec.subtocs):
                marks = list(spec.marks)
                if script.name == "wipe.sh":
                    marks.append(pytest.mark.xfail)  # TODO(TKR): FDB-652
                params.append(
                    pytest.param(
                        spec,
                        script,
                        id=f"{spec.id}::{script.name}",
                        marks=marks,
                    )
                )
        metafunc.parametrize(["fdb_env", "overlay_script"], params, indirect=["fdb_env"])
