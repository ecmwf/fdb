# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Shared gating for z3fdb tests that depend on optional build features."""

import pytest


def _has_gribjump_extractor() -> bool:
    """Return True if this build compiled the GribJump extractor.

    ``ExtractorType.GribJump`` is bound in every build so that user code does not depend on
    cmake flags, so its mere presence proves nothing — the extension module reports the build
    capability separately.
    """
    try:
        import chunked_data_view_bindings as pdv
    except ImportError:
        return False
    return bool(getattr(pdv, "has_gribjump_extractor", False))


HAS_GRIBJUMP_EXTRACTOR = _has_gribjump_extractor()

_GRIBJUMP_SKIP_REASON = (
    "build has no GribJump extractor; configure with -DENABLE_ZARR_GRIBJUMP_EXTRACTOR=ON "
    "(requires a bundle build providing gribjump)"
)


def pytest_runtest_setup(item):
    """Skip tests marked ``gribjump`` when the extractor was not compiled.

    Applies to every test under ``tests/z3fdb``, wherever it lives — mark an individual test
    with ``@pytest.mark.gribjump``, or a whole folder via a conftest that adds the marker (see
    ``integration/gribjump/conftest.py``).
    """
    if not HAS_GRIBJUMP_EXTRACTOR and item.get_closest_marker("gribjump") is not None:
        pytest.skip(_GRIBJUMP_SKIP_REASON)
