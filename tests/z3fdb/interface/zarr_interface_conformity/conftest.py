# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import pytest

from ._mocks import build_deep_store


def pytest_configure(config):
    # Automatically treat all async test functions in this directory as asyncio
    # tests without requiring an explicit @pytest.mark.asyncio on each one.
    config.option.asyncio_mode = "auto"


@pytest.fixture(scope="module")
def deep_store():
    """FdbZarrStore with a 4-level group/array hierarchy — see _mocks.build_deep_store."""
    return build_deep_store()


@pytest.fixture(scope="module")
def root_group(deep_store):
    """The root FdbZarrGroup of the deep_store fixture."""
    return deep_store._child
