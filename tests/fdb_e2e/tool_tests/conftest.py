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
    for case in ["info", "hide", "grib2fdb5", "list", "purge", "read", "root", "wipe", "write"]:
        if f"{case}_no_subtoc_script" in metafunc.fixturenames:
            # Collect your elements here
            elements = search_no_subtoc_test_sh_files(case)
            metafunc.parametrize(f"{case}_no_subtoc_script", elements, ids=[str(e.name) for e in elements])

        if f"{case}_subtoc_script" in metafunc.fixturenames:
            elements = search_subtoc_test_sh_files(case)
            metafunc.parametrize(f"{case}_subtoc_script", elements, ids=[str(e.name) for e in elements])

    # TODO(TKR): Handle overlay/wipe.sh separately due to FDB-652
    # Once the issue is fixed the lines below can be delete and "overlay" can be added to the case list
    # above
    if "overlay_no_subtoc_script" in metafunc.fixturenames:
        # Collect your elements here
        elements = search_no_subtoc_test_sh_files("overlay")

        marked_elements = []
        for el in elements:
            if "wipe.sh" in str(el):
                marked_elements.append(pytest.param(el, marks=[pytest.mark.xfail]))
            else:
                marked_elements.append(el)

        metafunc.parametrize("overlay_no_subtoc_script", marked_elements, ids=[str(e.name) for e in elements])

    if "overlay_subtoc_script" in metafunc.fixturenames:
        elements = search_subtoc_test_sh_files("overlay")
        marked_elements = []
        for el in elements:
            if "wipe.sh" in str(el):
                marked_elements.append(pytest.param(el.name, marks=[pytest.mark.xfail]))
            else:
                marked_elements.append(el)

        metafunc.parametrize("overlay_subtoc_script", marked_elements, ids=[str(e.name) for e in elements])
