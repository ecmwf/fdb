from pathlib import Path
import pytest
import git

import pathlib


def get_git_root(path) -> Path:
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return Path(git_root)


def search_test_sh_files_info():
    return sorted((get_git_root(__file__) / "tests" / "fdb_e2e" / "tool_tests" / "info").glob("**/*.sh"))


def search_test_sh_files_hide():
    return sorted((get_git_root(__file__) / "tests" / "fdb_e2e" / "tool_tests" / "hide").glob("**/*.sh"))


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
    if "info_script" in metafunc.fixturenames:
        # Collect your elements here
        elements = search_test_sh_files_info()  # your custom function

        metafunc.parametrize("info_script", elements, ids=[str(e) for e in elements])

    if "hide_script" in metafunc.fixturenames:
        # Collect your elements here
        elements = search_test_sh_files_hide()  # your custom function

        metafunc.parametrize("hide_script", elements, ids=[str(e) for e in elements])
