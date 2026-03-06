import pathlib
from pathlib import Path

import git
import pytest


def get_git_root(path) -> Path:
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return Path(git_root)


def search_no_subtoc_test_sh_files(case: str):
    return sorted((get_git_root(__file__) / "tests" / "fdb_e2e" / "tool_tests" / "no_subtocs" / case).glob("*.sh"))


def search_subtoc_test_sh_files(case: str):
    return sorted((get_git_root(__file__) / "tests" / "fdb_e2e" / "tool_tests" / "subtocs" / case).glob("*.sh"))


@pytest.fixture(scope="function")
def test_data_path() -> pathlib.Path:
    """
    Provides path to test data
    """

    git_repo = git.Repo(__file__, search_parent_directories=True)
    git_root = pathlib.Path(git_repo.git.rev_parse("--show-toplevel"))
    path = git_root / "tests" / "fdb_e2e" / "data"
    assert path.exists()
    return path


def pytest_generate_tests(metafunc):
    for case in ["info", "hide", "grib2fdb5", "list", "overlay", "purge", "read", "root", "wipe", "write"]:
        if f"{case}_no_subtoc_script" in metafunc.fixturenames:
            # Collect your elements here
            elements = search_no_subtoc_test_sh_files(case)
            metafunc.parametrize(f"{case}_no_subtoc_script", elements, ids=[str(e) for e in elements])
        if f"{case}_subtoc_script" in metafunc.fixturenames:
            elements = search_subtoc_test_sh_files(case)
            metafunc.parametrize(f"{case}_subtoc_script", elements, ids=[str(e) for e in elements])
