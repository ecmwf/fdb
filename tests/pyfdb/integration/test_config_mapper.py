# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import pytest
import yaml

from pyfdb._internal.pyfdb_internal import ConfigMapper


def test_config_mapper_wrong_type():
    with pytest.raises(ValueError):
        _ = ConfigMapper.to_json(0)


def test_config_mapper_none():
    json_config = ConfigMapper.to_json(None)
    assert json_config is None


def test_config_mapper_equality(read_only_fdb_setup):
    json_config_from_path = ConfigMapper.to_json(read_only_fdb_setup)
    json_config_from_str = ConfigMapper.to_json(read_only_fdb_setup.read_text())
    json_config_from_yaml = ConfigMapper.to_json(
        yaml.safe_load(read_only_fdb_setup.read_bytes())
    )

    assert json_config_from_path == json_config_from_str
    assert json_config_from_path == json_config_from_yaml
