# SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import logging

import pytest

from pychunked_data_view.exceptions import MarsRequestFormattingError
from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

from chunked_data_view_bindings import has_gribjump_extractor

logging.basicConfig(level=logging.DEBUG)

pytestmark = pytest.mark.offline


@pytest.mark.parametrize("invalid_axis", [3, 4])
def test_extend_on_invalid_axis_raises(
    read_only_fdb_pattern_setup,
    invalid_axis,
) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    # Three axis definitions ⇒ valid extension axis indices are 0..2.
    # Index 3 is the implicit field dimension (must not be used as extension
    # axis); index 4 is strictly out of bounds.
    builder.add_part(
        {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "time": [0, 6, 12, 18],
            "date": "2020-01-01/to/2020-01-02",
            "levtype": "sfc",
            "step": 0,
            "param": [165, 166, 167],
        },
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(invalid_axis)

    # pybind11's default translation of eckit::UserError surfaces as
    # RuntimeError, carrying the C++ exception's what() string.
    with pytest.raises(RuntimeError, match="not referring to a valid axis"):
        builder.build()


def test_wrong_key(
    read_only_fdb_pattern_setup,
) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    builder.add_part(
        {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "blubb": "0001",  # There is no blubb key
            "stream": "oper",
            "time": [18, 0, 12, 6],
            "date": ["2020-01-03", "2020-01-01", "2020-01-02"],
            "levtype": "sfc",
            "step": 0,
            "param": [167, 165, 166],
        },
        [
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.Grib(),
    )

    with pytest.raises(MarsRequestFormattingError):
        builder.build()


@pytest.mark.parametrize(
    ("bad_keys", "match"),
    [
        (["steps"], "steps"),  # single typo
        (["date", "times"], "times"),  # one correct, one typo
    ],
)
def test_axis_key_absent_from_request_raises(bad_keys, match) -> None:
    """AxisDefinition keys that are not present in the MARS request must raise
    ValueError at add_part time — before any FDB connection is attempted."""
    builder = SimpleStoreBuilder()
    with pytest.raises(ValueError, match=match):
        builder.add_part(
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "levtype": "sfc",
                "date": "2020-01-01",
                "time": 0,
                "step": 0,
                "param": 167,
            },
            [AxisDefinition(bad_keys, Chunking.SINGLE_VALUE)],
            ExtractorType.Grib(),
        )


@pytest.mark.skipif(
    has_gribjump_extractor,
    reason="build compiled the GribJump extractor",
)
def test_gribjump_without_build_support_raises(read_only_fdb_setup) -> None:
    """Using GribJump in a build without it must fail at build() and name the cmake flag.

    ``ExtractorType.GribJump`` is bound in every build so that user code does not depend on how
    fdb was compiled — which means the failure has to be actionable. This is the inverse of the
    ``gribjump`` marker: it runs exactly where integration/gribjump/ is skipped.
    """
    builder = SimpleStoreBuilder(read_only_fdb_setup)
    builder.add_part(
        {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "levtype": "sfc",
            "date": "2020-01-01",
            "time": 0,
            "step": 0,
            "param": 167,
        },
        [AxisDefinition(["param"], Chunking.SINGLE_VALUE)],
        ExtractorType.GribJump(),
    )
    with pytest.raises(RuntimeError, match="ENABLE_ZARR_GRIBJUMP_EXTRACTOR"):
        builder.build()
