import logging

import pytest

from pychunked_data_view.exceptions import MarsRequestFormattingError
from z3fdb import (
    AxisDefinition,
    ExtractorType,
    SimpleStoreBuilder,
)

logging.basicConfig(level=logging.DEBUG)


def test_additional_comma_end_of_request(
    read_only_fdb_pattern_setup,
) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    # The individual values are scrambled to check for persistent retrieval on the FDB side
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "time=18/0/12/6,"
        "date=2020-01-03/2020-01-01/2020-01-02,"
        "levtype=sfc,"
        "step=0,"
        "param=167/165/166,",  # Ending in an additional comma
        [
            AxisDefinition(["time"], True),
            AxisDefinition(["step"], True),
            AxisDefinition(["param"], True),
            AxisDefinition(["date"], True),
        ],
        ExtractorType.GRIB,
    )

    with pytest.raises(MarsRequestFormattingError):
        builder.build()


def test_missing_comma_between_keys(
    read_only_fdb_pattern_setup,
) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    # The individual values are scrambled to check for persistent retrieval on the FDB side
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "time=18/0/12/6"  # Missing comma here
        "date=2020-01-03/2020-01-01/2020-01-02,"
        "levtype=sfc,"
        "step=0,"
        "param=167/165/166",
        [
            AxisDefinition(["time"], True),
            AxisDefinition(["step"], True),
            AxisDefinition(["param"], True),
            AxisDefinition(["date"], True),
        ],
        ExtractorType.GRIB,
    )

    with pytest.raises(MarsRequestFormattingError):
        builder.build()


def test_wrong_key(
    read_only_fdb_pattern_setup,
) -> None:
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    # The individual values are scrambled to check for persistent retrieval on the FDB side
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "blubb=0001,"  # There is no blubb key
        "stream=oper,"
        "time=18/0/12/6,"
        "date=2020-01-03/2020-01-01/2020-01-02,"
        "levtype=sfc,"
        "step=0,"
        "param=167/165/166",
        [
            AxisDefinition(["time"], True),
            AxisDefinition(["step"], True),
            AxisDefinition(["param"], True),
            AxisDefinition(["date"], True),
        ],
        ExtractorType.GRIB,
    )

    with pytest.raises(MarsRequestFormattingError):
        builder.build()
