from pathlib import Path
import pytest
import yaml
from pyfdb._internal.pyfdb_internal import ConfigMapper


def test_config_mapper_wrong_type(read_only_fdb_setup):
    fdb_config_path: Path = read_only_fdb_setup

    assert fdb_config_path

    with pytest.raises(ValueError):
        _ = ConfigMapper.to_json(0)


def test_config_mapper_none(read_only_fdb_setup):
    fdb_config_path: Path = read_only_fdb_setup

    assert fdb_config_path

    json_config = ConfigMapper.to_json(None)

    assert json_config is None


def test_config_mapper_equality(read_only_fdb_setup):
    fdb_config_path: Path = read_only_fdb_setup

    assert fdb_config_path

    json_config_from_path = ConfigMapper.to_json(fdb_config_path)
    json_config_from_str = ConfigMapper.to_json(fdb_config_path.read_text())
    json_config_from_yaml = ConfigMapper.to_json(
        yaml.safe_load(fdb_config_path.read_bytes())
    )

    assert json_config_from_path == json_config_from_str
    assert json_config_from_path == json_config_from_yaml
