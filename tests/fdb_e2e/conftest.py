# list (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import json
import logging
import os
import pathlib
import shutil
from collections.abc import Generator

import git
import pytest
import yaml
from findlibs import Path

from pyfdb.pyfdb import FDB

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@pytest.fixture
def test_logger():
    return logger


@pytest.fixture(scope="function")
def data_path() -> pathlib.Path:
    """
    Provides path to test data
    """
    path = pathlib.Path(__file__).resolve().parent / "data"
    assert path.exists()
    return path


@pytest.fixture(scope="function")
def test_data_path() -> pathlib.Path:
    """
    Provides path to test data
    """

    git_repo = git.Repo(__file__, search_parent_directories=True)
    git_root = pathlib.Path(git_repo.git.rev_parse("--show-toplevel"))
    path = git_root / "tests" / "fdb" / "e2e" / "data"
    assert path.exists()
    return path


@pytest.fixture(scope="function")
def get_git_root() -> Path:
    git_repo = git.Repo(__file__, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return Path(git_root)


@pytest.fixture(scope="session")
def session_tmp(tmp_path_factory) -> Generator[pathlib.Path, None, None]:
    tmp_dir = tmp_path_factory.mktemp("session_data")
    yield tmp_dir


@pytest.fixture(scope="function")
def function_tmp(tmp_path_factory) -> Generator[pathlib.Path, None, None]:
    tmp_function_dir = tmp_path_factory.mktemp("test_data_", numbered=True)
    yield tmp_function_dir


@pytest.fixture(scope="function", autouse=False)
def empty_fdb_setup(data_path, function_tmp) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    This setup can be shared between tests
    """
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()

    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    schema_path = fdb_config_dir / "schema"
    shutil.copy(schema_path_src, schema_path)

    fdb5_root = function_tmp / "root"
    fdb5_root.mkdir()
    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "handler": "Default",
                "roots": [
                    {"path": str(fdb5_root)},
                ],
            }
        ],
    }
    fdb_config_str = yaml.dump(fdb_config)
    fdb_config_path = fdb_config_dir / "fdb_config.yaml"
    fdb_config_path.write_text(fdb_config_str)
    return fdb_home_dir


def simple_fdb_setup(data_path, function_tmp, subtocs: bool, expver_handler: bool) -> tuple[pathlib.Path, pathlib.Path]:
    """
    Creates a FDB setup in this tests temp directory.
    This setup can be shared between tests

    This is a setup which creates a fdb config file. The schema file need to
    be placed in etc/fdb subfolder but the config can be located in the root.
    """

    fdb_home_dir = function_tmp

    # Copy schema to config folder
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()

    schema_dir = fdb_home_dir / "etc" / "fdb"
    schema_dir.mkdir(parents=True)

    schema_path = fdb_home_dir / "etc" / "fdb" / "schema"
    shutil.copy(schema_path_src, schema_path)

    invalid_root_path = fdb_home_dir / "invalid-root"

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "regex": "rd:?.*",
                "handler": "expver" if expver_handler is True else "Default",
                "roots": [
                    {"path": str(invalid_root_path)},
                    {"path": str(root_path)},
                ],
            }
        ],
    }

    fdb_config_dir = fdb_home_dir  # Config file in home dir, env contains the FDB5_CONFIG_PATH

    fdb_config_str = yaml.dump(fdb_config)
    fdb_config_path = fdb_config_dir / "config.yaml"
    fdb_config_path.write_text(fdb_config_str)

    fdb_user_config = {"useSubToc": subtocs}
    fdb_user_config_str = yaml.dump(fdb_user_config)
    fdb_user_config_path = fdb_config_dir / "user_config.yaml"
    fdb_user_config_path.write_text(fdb_user_config_str)

    return fdb_config_path, fdb_user_config_path


@pytest.fixture(scope="function", autouse=False)
def simple_fdb_setup_no_subtocs_no_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = simple_fdb_setup(data_path, function_tmp, subtocs=False, expver_handler=False)

    os.environ["FDB_HOME"] = str(function_tmp)
    os.environ["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    fdb = FDB()

    del os.environ["FDB_HOME"]
    del os.environ["FDB5_CONFIG_FILE"]

    # Check that the temporary fdb is configured and picked up
    assert str(function_tmp) in fdb.config()[0]["schema"]
    all(str(function_tmp) in root["path"] for root in fdb.config()[0]["spaces"][0]["roots"])

    return fdb


@pytest.fixture(scope="function", autouse=False)
def simple_env_no_subtocs_no_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)
    env["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    return env


@pytest.fixture(scope="function", autouse=False)
def simple_fdb_setup_no_subtocs_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = simple_fdb_setup(data_path, function_tmp, subtocs=False, expver_handler=True)

    os.environ["FDB_HOME"] = str(function_tmp)
    os.environ["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    fdb = FDB()

    del os.environ["FDB_HOME"]
    del os.environ["FDB5_CONFIG_FILE"]

    # Check that the temporary fdb is configured and picked up
    assert str(function_tmp) in fdb.config()[0]["schema"]
    all(str(function_tmp) in root["path"] for root in fdb.config()[0]["spaces"][0]["roots"])

    return fdb


@pytest.fixture(scope="function", autouse=False)
def simple_env_no_subtocs_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)
    env["FDB_EXPVER_FILE"] = f"{str(function_tmp)}/expver.map"
    env["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    file: Path = Path(function_tmp / "expver.map")
    file.touch()

    assert file.exists()

    return env


@pytest.fixture(scope="function", autouse=False)
def simple_fdb_setup_subtocs_no_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = simple_fdb_setup(data_path, function_tmp, subtocs=True, expver_handler=False)

    os.environ["FDB_HOME"] = str(function_tmp)
    os.environ["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    fdb = FDB()

    del os.environ["FDB_HOME"]
    del os.environ["FDB5_CONFIG_FILE"]

    # Check that the temporary fdb is configured and picked up
    assert str(function_tmp) in fdb.config()[0]["schema"]
    all(str(function_tmp) in root["path"] for root in fdb.config()[0]["spaces"][0]["roots"])

    return fdb


@pytest.fixture(scope="function", autouse=False)
def simple_env_subtocs_no_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)
    env["FDB5_SUB_TOCS"] = "1"
    env["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    return env


@pytest.fixture(scope="function", autouse=False)
def simple_fdb_setup_subtocs_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = simple_fdb_setup(data_path, function_tmp, subtocs=True, expver_handler=True)

    os.environ["FDB_HOME"] = str(function_tmp)
    os.environ["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    fdb = FDB()

    del os.environ["FDB_HOME"]
    del os.environ["FDB5_CONFIG_FILE"]

    # Check that the temporary fdb is configured and picked up
    assert str(function_tmp) in fdb.config()[0]["schema"]
    all(str(function_tmp) in root["path"] for root in fdb.config()[0]["spaces"][0]["roots"])

    return fdb


@pytest.fixture(scope="function", autouse=False)
def simple_env_subtocs_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)
    env["FDB_EXPVER_FILE"] = f"{str(function_tmp)}/expver.map"
    env["FDB5_SUB_TOCS"] = "1"
    env["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    file: Path = Path(function_tmp / "expver.map")
    file.touch()

    assert file.exists()

    return env


def files_fdb_setup(
    data_path, function_tmp, subtocs: bool, expver_handler: bool
) -> tuple[pathlib.Path | None, pathlib.Path | None]:
    """
    Creates a FDB setup in this tests temp directory.
    This setup can be shared between tests

    The main intention is to check whether the setup of the FDB is possible without supplying
    a config file. If no config file is given we need to create the corresponding files in the
    fdb_config_dir.
    """

    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    # Copy schema to config folder
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()
    schema_path = fdb_config_dir / "schema"
    shutil.copy(schema_path_src, schema_path)

    # invalid_root_path = fdb_home_dir / "invalid-root"

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    fdb_spaces_path = fdb_config_dir / "spaces"
    if expver_handler is True:
        fdb_spaces_path.write_text("rd:?.*               rd      expver")
    else:
        fdb_spaces_path.write_text("rd:?.*               rd      Default")

    fdb_roots_path: Path = fdb_config_dir / "roots"

    with fdb_roots_path.open("w+") as file_fdb_roots:
        file_fdb_roots.write(f"{str(fdb_home_dir)}/root            rd     yes yes" + os.linesep)
        if expver_handler:
            fdb_roots_path.write_text(f"{str(fdb_home_dir)}/invalid-root            rd     yes yes")
        else:
            fdb_roots_path.write_text(f"{str(fdb_home_dir)}/invalid-root            rd     no no")

    return None, None


@pytest.fixture(scope="function", autouse=False)
def files_fdb_setup_no_subtocs_no_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = files_fdb_setup(data_path, function_tmp, subtocs=False, expver_handler=False)

    os.environ["FDB_HOME"] = str(function_tmp)
    fdb = FDB()
    del os.environ["FDB_HOME"]

    config, user_config = fdb.config()

    # Check that there is no config found
    assert config == {}
    assert user_config == {}

    return fdb


@pytest.fixture(scope="function", autouse=False)
def files_env_no_subtocs_no_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)

    return env


@pytest.fixture(scope="function", autouse=False)
def files_fdb_setup_no_subtocs_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = files_fdb_setup(data_path, function_tmp, subtocs=False, expver_handler=True)

    os.environ["FDB_HOME"] = str(function_tmp)
    fdb = FDB()
    del os.environ["FDB_HOME"]

    config, user_config = fdb.config()

    # Check that there is no config found
    assert config == {}
    assert user_config == {}

    return fdb


@pytest.fixture(scope="function", autouse=False)
def files_env_no_subtocs_expver_handler(function_tmp) -> dict[str, str]:
    fdb_schema_dir = function_tmp / "etc" / "fdb"

    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)
    env["FDB5_ROOT"] = str(function_tmp / "root")
    env["FDB_EXPVER_FILE"] = f"{str(fdb_schema_dir / 'expver.map')}"

    file: Path = Path(fdb_schema_dir / "expver.map")
    file.touch()

    assert file.exists()

    return env


@pytest.fixture(scope="function", autouse=False)
def files_fdb_setup_subtocs_no_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = files_fdb_setup(data_path, function_tmp, subtocs=True, expver_handler=False)
    os.environ["FDB_HOME"] = str(function_tmp)
    fdb = FDB()
    del os.environ["FDB_HOME"]

    config, user_config = fdb.config()

    # Check that there is no config found
    assert config == {}
    assert user_config == {}

    return fdb


@pytest.fixture(scope="function", autouse=False)
def files_env_subtocs_no_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)
    env["FDB5_SUB_TOCS"] = "1"

    return env


@pytest.fixture(scope="function", autouse=False)
def files_fdb_setup_subtocs_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = files_fdb_setup(data_path, function_tmp, subtocs=True, expver_handler=True)

    os.environ["FDB_HOME"] = str(function_tmp)
    fdb = FDB()
    del os.environ["FDB_HOME"]

    config, user_config = fdb.config()

    # Check that there is no config found
    assert config == {}
    assert user_config == {}

    return fdb


@pytest.fixture(scope="function", autouse=False)
def files_env_subtocs_expver_handler(function_tmp) -> dict[str, str]:
    fdb_schema_dir = function_tmp / "etc" / "fdb"

    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)
    env["FDB5_ROOT"] = str(function_tmp / "root")
    env["FDB5_SUB_TOCS"] = "1"
    env["FDB_EXPVER_FILE"] = f"{str(fdb_schema_dir / 'expver.map')}"

    file: Path = Path(fdb_schema_dir / "expver.map")
    file.touch()

    assert file.exists()

    return env


def config_yaml_fdb_setup(
    data_path, function_tmp, subtocs: bool, expver_handler: bool
) -> tuple[pathlib.Path, pathlib.Path | None]:
    """
    Creates a FDB setup in this tests temp directory.
    This setup can be shared between tests

    All the needed config files are located in etc/fdb subfolder
    of the temporary work directory.

    The only env variable which is set should be FDB_HOME, so no
    FDB5_CONFIG_PATH.
    """

    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    # Copy schema to config folder
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()
    schema_path = fdb_config_dir / "schema"
    shutil.copy(schema_path_src, schema_path)

    invalid_root_path = fdb_home_dir / "invalid-root"

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "regex": "rd:?.*",
                "handler": "expver" if expver_handler is True else "Default",
                "roots": [
                    {"path": str(invalid_root_path)},
                    {"path": str(root_path)},
                ],
            }
        ],
    }

    fdb_config_str = yaml.dump(fdb_config)
    fdb_config_path = fdb_config_dir / "config.yaml"
    fdb_config_path.write_text(fdb_config_str)

    return fdb_config_path, None


@pytest.fixture(scope="function", autouse=False)
def config_yaml_env_no_subtocs_no_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)

    return env


@pytest.fixture(scope="function", autouse=False)
def config_yaml_fdb_setup_no_subtocs_no_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = config_yaml_fdb_setup(data_path, function_tmp, subtocs=False, expver_handler=False)

    os.environ["FDB_HOME"] = str(function_tmp)
    fdb = FDB()
    del os.environ["FDB_HOME"]

    config, user_config = fdb.config()

    # Check that there a config found, the one in etc/fdb
    assert config != {}
    assert user_config == {}

    return fdb


def config_json_fdb_setup(
    data_path, function_tmp, subtocs: bool, expver_handler: bool
) -> tuple[pathlib.Path, pathlib.Path | None]:
    """
    Creates a FDB setup in this tests temp directory.
    This setup can be shared between tests

    All the needed config files are located in etc/fdb subfolder
    of the temporary work directory.

    The only env variable which is set should be FDB_HOME, so no
    FDB5_CONFIG_PATH.
    """

    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    # Copy schema to config folder
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()
    schema_path = fdb_config_dir / "schema"
    shutil.copy(schema_path_src, schema_path)

    invalid_root_path = fdb_home_dir / "invalid-root"

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "regex": "rd:?.*",
                "handler": "expver" if expver_handler is True else "Default",
                "roots": [
                    {"path": str(invalid_root_path)},
                    {"path": str(root_path)},
                ],
            }
        ],
    }

    fdb_config_str = json.dumps(fdb_config)
    fdb_config_path = fdb_config_dir / "config.json"
    fdb_config_path.write_text(fdb_config_str)

    return fdb_config_path, None


@pytest.fixture(scope="function", autouse=False)
def config_json_env_no_subtocs_no_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)

    return env


@pytest.fixture(scope="function", autouse=False)
def config_json_fdb_setup_no_subtocs_no_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = config_json_fdb_setup(data_path, function_tmp, subtocs=False, expver_handler=False)

    os.environ["FDB_HOME"] = str(function_tmp)
    fdb = FDB()
    del os.environ["FDB_HOME"]

    config, user_config = fdb.config()

    # Check that there a config found, the one in etc/fdb
    assert config != {}
    assert user_config == {}

    return fdb


def config_mars_disks_fdb_setup(
    data_path, function_tmp, subtocs: bool, expver_handler: bool
) -> tuple[pathlib.Path, pathlib.Path | None]:
    """
    Creates a FDB setup in this tests temp directory.
    This setup can be shared between tests

    All the needed config files are located in etc/fdb subfolder
    of the temporary work directory.

    The only env variable which is set should be FDB_HOME, so no
    FDB5_CONFIG_PATH.
    """

    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    # Copy schema to config folder
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()
    schema_path = fdb_config_dir / "schema"
    shutil.copy(schema_path_src, schema_path)

    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "marsDisks": True,
                "name": "marsFdb",
                "regex": "r.*",
                "handler": "WeightedRandom",
            }
        ],
    }

    fdb_config_str = yaml.dump(fdb_config)
    fdb_config_path = fdb_config_dir / "config.yaml"
    fdb_config_path.write_text(fdb_config_str)

    mars_disks_path = fdb_home_dir / "etc" / "disks"
    mars_disks_path.mkdir(parents=True)

    (mars_disks_path / "fdb").write_text(f"{str(fdb_home_dir)}/root")

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    return fdb_config_path, None


@pytest.fixture(scope="function", autouse=False)
def config_yaml_mars_disks_env_no_subtocs_no_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)

    return env


@pytest.fixture(scope="function", autouse=False)
def config_yaml_mars_disks_fdb_setup_no_subtocs_no_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = config_mars_disks_fdb_setup(data_path, function_tmp, subtocs=False, expver_handler=False)

    os.environ["FDB_HOME"] = str(function_tmp)
    fdb = FDB()
    del os.environ["FDB_HOME"]

    config, user_config = fdb.config()

    # Check that there a config found, the one in etc/fdb
    assert config != {}
    assert user_config == {}

    return fdb


def config_tools_fdb_setup(
    data_path, function_tmp, dump_json: bool = False
) -> tuple[pathlib.Path, pathlib.Path | None]:
    """
    Creates a FDB setup in this tests temp directory.
    This setup can be shared between tests

    All the needed config files are located in etc/fdb subfolder
    of the temporary work directory.

    The only env variable which is set should be FDB_HOME, so no
    FDB5_CONFIG_PATH.

    Write the config as yaml per default and json if the corresponding flag is supplied
    """

    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    # Copy schema to config folder
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()
    schema_path = fdb_config_dir / "schema"
    shutil.copy(schema_path_src, schema_path)

    invalid_root_path = fdb_home_dir / "invalid-root"

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "regex": "rd:?.*",
                "handler": "Default",
                "roots": [
                    {"path": str(invalid_root_path)},
                    {"path": str(root_path)},
                ],
            }
        ],
    }

    tool_names = [
        "base_config",
        "fdb-root",
        "fdb-info",
        "fdb-write",
        "fdb-purge",
        "fdb-wipe",
        "fdb-list",
        "fdb-read",
        "fdb-hide",
        "fdb-overlay",
        "fdb-write-legacy",
        "fdb-where",
        "fdb-adopt",
        "grib2fdb5",
    ]

    for tool_name in tool_names:
        if dump_json is False:
            fdb_config_str = yaml.dump(fdb_config)
            fdb_config_path = fdb_config_dir / f"{tool_name}.yaml"
            fdb_config_path.write_text(fdb_config_str)
        else:
            fdb_config_str = json.dumps(fdb_config)
            fdb_config_path = fdb_config_dir / f"{tool_name}.json"
            fdb_config_path.write_text(fdb_config_str)

    fdb_config_path = fdb_config_dir / "base_config.yaml"

    return fdb_config_path, None


@pytest.fixture(scope="function", autouse=False)
def config_yaml_tools_env_no_subtocs_no_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)

    return env


@pytest.fixture(scope="function", autouse=False)
def config_yaml_tools_fdb_setup_no_subtocs_no_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = config_tools_fdb_setup(data_path, function_tmp, dump_json=False)

    os.environ["FDB_HOME"] = str(function_tmp)
    fdb = FDB()
    del os.environ["FDB_HOME"]

    config, user_config = fdb.config()

    # Check that there a config found, the one in etc/fdb
    assert config == {}
    assert user_config == {}

    return fdb


@pytest.fixture(scope="function", autouse=False)
def config_json_tools_env_no_subtocs_no_expver_handler(function_tmp) -> dict[str, str]:
    env = {}
    env["PATH"] = os.environ["PATH"]
    env["FDB_HOME"] = str(function_tmp)

    return env


@pytest.fixture(scope="function", autouse=False)
def config_json_tools_fdb_setup_no_subtocs_no_expver_handler(data_path, function_tmp) -> FDB:
    _, _ = config_tools_fdb_setup(data_path, function_tmp, dump_json=True)

    os.environ["FDB_HOME"] = str(function_tmp)
    fdb = FDB()
    del os.environ["FDB_HOME"]

    config, user_config = fdb.config()

    # Check that there a config found, the one in etc/fdb
    assert config == {}
    assert user_config == {}

    return fdb
