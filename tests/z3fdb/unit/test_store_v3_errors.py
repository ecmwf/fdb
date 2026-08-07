import logging

import pytest

from pychunked_data_view.exceptions import MarsRequestFormattingError
from z3fdb import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    SimpleStoreBuilder,
)

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
            "type": "an", "class": "ea", "domain": "g", "expver": "0001",
            "stream": "oper", "time": [0, 6, 12, 18],
            "date": "2020-01-01/to/2020-01-02",
            "levtype": "sfc", "step": 0, "param": [165, 166, 167],
        },
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
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
            "type": "an", "class": "ea", "domain": "g",
            "blubb": "0001",  # There is no blubb key
            "stream": "oper", "time": [18, 0, 12, 6],
            "date": ["2020-01-03", "2020-01-01", "2020-01-02"],
            "levtype": "sfc", "step": 0, "param": [167, 165, 166],
        },
        [
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )

    with pytest.raises(MarsRequestFormattingError):
        builder.build()
