# (C) Copyright 2011- ECMWF.
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
from dataclasses import dataclass

import git
import pytest
import yaml
from findlibs import Path

from pyfdb.pyfdb import FDB

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# Command-line options
def pytest_addoption(parser):
    parser.addoption(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra environment variable forwarded to shell scripts (repeatable).",
    )


# Core data structures
@dataclass(frozen=True)
class FdbEnvSpec:
    """Declarative description of one FDB configuration variant.

    Adding a new local configuration is a single entry in ALL_LOCAL_SPECS.
    Adding a new *mode* (e.g. remote) requires one entry in ALL_LOCAL_SPECS
    (or a separate list) and a builder registered in _BUILDERS.
    """

    id: str  # short test ID shown in pytest output
    config_style: str  # selects the builder: "simple"|"files"|"yaml"|"json"|...
    subtocs: bool  # whether FDB5_SUB_TOCS is enabled
    expver_handler: bool  # whether the expver file handler is active
    marks: tuple  # pytest marks applied to each test instance


@dataclass
class FdbEnvironment:
    """Everything a test needs to run against one FDB configuration.

    fdb:  pyfdb handle for Python-API tests (None when not applicable).
    env:  subprocess environment dict for shell-script tests.
    tmp:  per-test temp directory (also the working directory for scripts).
    spec: the FdbEnvSpec that produced this environment.
    """

    fdb: FDB | None
    env: dict[str, str]
    tmp: pathlib.Path
    spec: FdbEnvSpec


# All local configurations — single source of truth
#
# To add a new local configuration variant: add one line here.
# To add a new mode entirely (e.g. remote): add a new list and register a
# builder in _BUILDERS; no test-function changes needed.
# fmt: off
ALL_LOCAL_SPECS: list[FdbEnvSpec] = [
    # simple: config.yaml placed at FDB_HOME root; FDB5_CONFIG_FILE set explicitly
    FdbEnvSpec("simple",                "simple", subtocs=False, expver_handler=False, marks=(pytest.mark.simple,)),
    FdbEnvSpec("simple_expver",         "simple", subtocs=False, expver_handler=True,  marks=(pytest.mark.simple,)),
    FdbEnvSpec("simple_subtocs",        "simple", subtocs=True,  expver_handler=False, marks=(pytest.mark.simple,)),
    FdbEnvSpec("simple_subtocs_expver", "simple", subtocs=True,  expver_handler=True,  marks=(pytest.mark.simple,)),
    # files: old-style flat files in etc/fdb/{spaces,roots}; no YAML config
    FdbEnvSpec("files",                 "files",  subtocs=False, expver_handler=False, marks=(pytest.mark.files,)),
    FdbEnvSpec("files_expver",          "files",  subtocs=False, expver_handler=True,  marks=(pytest.mark.files,)),
    FdbEnvSpec("files_subtocs",         "files",  subtocs=True,  expver_handler=False, marks=(pytest.mark.files,)),
    FdbEnvSpec("files_subtocs_expver",  "files",  subtocs=True,  expver_handler=True,  marks=(pytest.mark.files,)),
    # yaml / json: config in etc/fdb/; only FDB_HOME needed (no FDB5_CONFIG_FILE)
    FdbEnvSpec("yaml",           "yaml",       subtocs=False, expver_handler=False, marks=(pytest.mark.yaml,)),
    FdbEnvSpec("json",           "json",       subtocs=False, expver_handler=False, marks=(pytest.mark.json,)),
    # mars_disks: WeightedRandom space backed by an etc/disks/fdb disk-list file
    FdbEnvSpec("yaml_mars_disks", "mars_disks", subtocs=False, expver_handler=False, marks=(pytest.mark.yaml_mars_disks,)),
    # yaml_tools / json_tools: one config file per tool name in etc/fdb/
    FdbEnvSpec("yaml_tools", "yaml_tools", subtocs=False, expver_handler=False, marks=(pytest.mark.yaml_tools,)),
    FdbEnvSpec("json_tools", "json_tools", subtocs=False, expver_handler=False, marks=(pytest.mark.json_tools,)),
]
# fmt: on


# Infrastructure fixtures
@pytest.fixture(scope="session")
def cli_env(request) -> dict[str, str]:
    """Environment variables supplied via --env KEY=VALUE on the command line."""
    result = {}
    for entry in request.config.getoption("--env"):
        key, _, value = entry.partition("=")
        result[key] = value
    return result


@pytest.fixture
def test_logger():
    return logger


@pytest.fixture(scope="function")
def data_path() -> pathlib.Path:
    """Provides path to the fdb_e2e test data directory."""
    path = pathlib.Path(__file__).resolve().parent / "data"
    assert path.exists()
    return path


@pytest.fixture(scope="function")
def test_data_path() -> pathlib.Path:
    """Provides path to test data (resolved via git root)."""
    git_repo = git.Repo(__file__, search_parent_directories=True)
    git_root = pathlib.Path(git_repo.git.rev_parse("--show-toplevel"))
    path = git_root / "tests" / "fdb_e2e" / "data"
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
    tmp_function_dir = tmp_path_factory.mktemp("pytest-tmp", numbered=True)
    yield tmp_function_dir


# Config builder helpers (plain functions, not fixtures)
#
# Each builder writes config files into function_tmp.
# Uniform signature: (data_path, function_tmp, subtocs, expver_handler) -> None
def simple_fdb_setup(
    data_path, function_tmp, subtocs: bool, expver_handler: bool
) -> None:
    """config.yaml at FDB_HOME root; caller must set FDB5_CONFIG_FILE."""
    fdb_home_dir = function_tmp

    schema_dir = fdb_home_dir / "etc" / "fdb"
    schema_dir.mkdir(parents=True)
    schema_path = schema_dir / "schema"
    shutil.copy(data_path / "schema", schema_path)

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "regex": "rd:?.*",
                "handler": "expver" if expver_handler else "Default",
                "roots": [
                    {"path": str(fdb_home_dir / "invalid-root")},
                    {"path": str(root_path)},
                ],
            }
        ],
    }
    (fdb_home_dir / "config.yaml").write_text(yaml.dump(fdb_config))
    (fdb_home_dir / "user_config.yaml").write_text(yaml.dump({"useSubToc": subtocs}))


def files_fdb_setup(
    data_path, function_tmp, subtocs: bool, expver_handler: bool
) -> None:
    """Old-style flat config in etc/fdb/{spaces,roots}; no YAML config."""
    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    shutil.copy(data_path / "schema", fdb_config_dir / "schema")

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    spaces_text = (
        "rd:?.*               rd      expver"
        if expver_handler
        else "rd:?.*               rd      Default"
    )
    (fdb_config_dir / "spaces").write_text(spaces_text)

    fdb_roots_path = fdb_config_dir / "roots"
    invalid_suffix = "yes yes" if expver_handler else "no no"
    fdb_roots_path.write_text(
        f"{fdb_home_dir}/root            rd     yes yes\n"
        f"{fdb_home_dir}/invalid-root            rd     {invalid_suffix}\n"
    )


def config_yaml_fdb_setup(
    data_path, function_tmp, subtocs: bool, expver_handler: bool
) -> None:
    """config.yaml in etc/fdb/; only FDB_HOME needed."""
    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    schema_path = fdb_config_dir / "schema"
    shutil.copy(data_path / "schema", schema_path)

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "regex": "rd:?.*",
                "handler": "expver" if expver_handler else "Default",
                "roots": [
                    {"path": str(fdb_home_dir / "invalid-root")},
                    {"path": str(root_path)},
                ],
            }
        ],
    }
    (fdb_config_dir / "config.yaml").write_text(yaml.dump(fdb_config))


def config_json_fdb_setup(
    data_path, function_tmp, subtocs: bool, expver_handler: bool
) -> None:
    """config.json in etc/fdb/; only FDB_HOME needed."""
    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    schema_path = fdb_config_dir / "schema"
    shutil.copy(data_path / "schema", schema_path)

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "regex": "rd:?.*",
                "handler": "expver" if expver_handler else "Default",
                "roots": [
                    {"path": str(fdb_home_dir / "invalid-root")},
                    {"path": str(root_path)},
                ],
            }
        ],
    }
    (fdb_config_dir / "config.json").write_text(json.dumps(fdb_config))


def config_mars_disks_fdb_setup(
    data_path, function_tmp, subtocs: bool, expver_handler: bool
) -> None:
    """WeightedRandom space backed by an etc/disks/fdb disk-list file."""
    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    schema_path = fdb_config_dir / "schema"
    shutil.copy(data_path / "schema", schema_path)

    root_path = fdb_home_dir / "root"
    root_path.mkdir()

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
    (fdb_config_dir / "config.yaml").write_text(yaml.dump(fdb_config))

    mars_disks_path = fdb_home_dir / "etc" / "disks"
    mars_disks_path.mkdir(parents=True)
    (mars_disks_path / "fdb").write_text(str(root_path))


def config_tools_fdb_setup(data_path, function_tmp, dump_json: bool = False) -> None:
    """One config file per named FDB tool in etc/fdb/."""
    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)

    schema_path = fdb_config_dir / "schema"
    shutil.copy(data_path / "schema", schema_path)

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
                    {"path": str(fdb_home_dir / "invalid-root")},
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
    ext = ".json" if dump_json else ".yaml"
    serialise = json.dumps if dump_json else yaml.dump
    for tool_name in tool_names:
        (fdb_config_dir / f"{tool_name}{ext}").write_text(serialise(fdb_config))


# Dispatch table: config_style → builder
#
# Uniform signature: (data_path, function_tmp, subtocs, expver_handler) -> None
#
# Extension point: register a new config style or an entirely new mode here
# (e.g. "remote": remote_fdb_setup) without touching any test functions.
_BUILDERS: dict = {
    "simple": simple_fdb_setup,
    "files": files_fdb_setup,
    "yaml": config_yaml_fdb_setup,
    "json": config_json_fdb_setup,
    "mars_disks": config_mars_disks_fdb_setup,
    "yaml_tools": lambda d, t, sub, exp: config_tools_fdb_setup(d, t, dump_json=False),
    "json_tools": lambda d, t, sub, exp: config_tools_fdb_setup(d, t, dump_json=True),
}


# Internal helpers
def _create_and_validate_fdb(spec: FdbEnvSpec, function_tmp: pathlib.Path) -> FDB:
    """Instantiate pyfdb.FDB() under the right env vars and validate the result."""
    env_additions: dict[str, str] = {"FDB_HOME": str(function_tmp)}
    if spec.config_style == "simple":
        env_additions["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    saved = {k: os.environ.get(k) for k in env_additions}
    try:
        os.environ.update(env_additions)
        fdb = FDB()
    finally:
        for k, old in saved.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old

    config, user_config = fdb.config()

    if spec.config_style == "simple":
        assert str(function_tmp) in config["schema"]
        all(str(function_tmp) in root["path"] for root in config["spaces"][0]["roots"])
    elif spec.config_style in ("files", "yaml_tools", "json_tools"):
        assert config == {}
        assert user_config == {}
    else:  # yaml, json, mars_disks
        assert config != {}
        assert user_config == {}

    return fdb


def _build_subprocess_env(
    spec: FdbEnvSpec, function_tmp: pathlib.Path, cli_env: dict[str, str]
) -> dict[str, str]:
    """Build the env dict forwarded to shell-script subprocesses."""
    env: dict[str, str] = {
        "PATH": os.environ["PATH"],
        "FDB_HOME": str(function_tmp),
    }

    if spec.config_style == "simple":
        env["FDB5_CONFIG_FILE"] = str(function_tmp / "config.yaml")

    if spec.config_style == "files" and spec.expver_handler:
        env["FDB5_ROOT"] = str(function_tmp / "root")

    if spec.subtocs:
        env["FDB5_SUB_TOCS"] = "1"

    if spec.expver_handler:
        # files-style keeps the expver map alongside the other flat config files
        if spec.config_style == "files":
            expver_file = function_tmp / "etc" / "fdb" / "expver.map"
        else:
            expver_file = function_tmp / "expver.map"
        expver_file.touch()
        env["FDB_EXPVER_FILE"] = str(expver_file)

    env.update(cli_env)
    return env


def _build_local_environment(
    spec: FdbEnvSpec,
    function_tmp: pathlib.Path,
    data_path: pathlib.Path,
    cli_env: dict[str, str],
) -> FdbEnvironment:
    """Construct a local FdbEnvironment from an FdbEnvSpec."""
    builder = _BUILDERS[spec.config_style]
    builder(data_path, function_tmp, spec.subtocs, spec.expver_handler)
    fdb = _create_and_validate_fdb(spec, function_tmp)
    env = _build_subprocess_env(spec, function_tmp, cli_env)
    return FdbEnvironment(fdb=fdb, env=env, tmp=function_tmp, spec=spec)


# The single FDB environment fixture
@pytest.fixture
def fdb_env(request, function_tmp, data_path, cli_env) -> FdbEnvironment:
    """FDB environment fixture — used as an *indirect* parametrised fixture.

    pytest_generate_tests in tool_tests/conftest.py drives the parametrisation
    by pairing each FdbEnvSpec with the scripts that match its subtoc mode.
    This avoids the full cartesian product (spec × all scripts) and the
    associated runtime skips.
    """
    spec: FdbEnvSpec = request.param
    return _build_local_environment(spec, function_tmp, data_path, cli_env)


# Legacy fixture kept for other tests that may reference it
@pytest.fixture(scope="function", autouse=False)
def empty_fdb_setup(data_path, function_tmp) -> pathlib.Path:
    """Creates a minimal FDB setup in the test's temp directory."""
    fdb_home_dir = function_tmp
    fdb_config_dir = fdb_home_dir / "etc" / "fdb"
    fdb_config_dir.mkdir(parents=True)
    schema_path = fdb_config_dir / "schema"
    shutil.copy(data_path / "schema", schema_path)

    fdb5_root = function_tmp / "root"
    fdb5_root.mkdir()
    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [{"handler": "Default", "roots": [{"path": str(fdb5_root)}]}],
    }
    fdb_config_path = fdb_config_dir / "fdb_config.yaml"
    fdb_config_path.write_text(yaml.dump(fdb_config))
    return fdb_home_dir
