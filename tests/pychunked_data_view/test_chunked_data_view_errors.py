# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import pytest

from pychunked_data_view import (
    AxisDefinition,
    ChunkedDataViewBuilder,
    Chunking,
    ExtractorType,
)
from pychunked_data_view.exceptions import InternalError, MarsRequestFormattingError

pytestmark = pytest.mark.offline


@pytest.mark.parametrize(
    "malformed_request, expected_hint",
    [
        # Trailing comma — triggers StreamParser::next in the eckit MARS parser
        ("class=ea,", "Did the MARS request end in a comma"),
        # Missing comma between key=value pairs — triggers MarsParser::parseVerb
        ("class=ea,,domain=g", "Did you miss a comma between keys"),
    ],
)
def test_malformed_mars_string_raises(
    read_only_fdb_setup, malformed_request, expected_hint
) -> None:
    """Malformed raw MARS request strings are caught in build() and re-raised as
    MarsRequestFormattingError with an actionable hint.

    The Python add_part() API only accepts dicts and cannot produce such strings;
    we bypass it here by calling the underlying C++ builder directly so that the
    error-detection logic in build() can be exercised.
    """
    builder = ChunkedDataViewBuilder(read_only_fdb_setup)
    # Inject the malformed MARS string at the bindings level — addPart() stores it
    # without parsing, so no error fires here; it fires in build().
    builder._obj.add_part(malformed_request, [], ExtractorType.GRIB.value)
    with pytest.raises(MarsRequestFormattingError, match=expected_hint):
        builder.build()


def test_misspelled_mars_key_raises(read_only_fdb_setup) -> None:
    """An unrecognised MARS key causes build() to raise MarsRequestFormattingError
    with a hint about the misspelling.

    metkit's type-expansion (FDBToolRequest::requestsFromString) rejects the key
    with 'Cannot match <key>' before any FDB retrieval is attempted.
    """
    builder = ChunkedDataViewBuilder(read_only_fdb_setup)
    builder.add_part(
        {
            "class": "ea",
            "type": "an",
            "expver": "0001",
            "domain": "g",
            "stream": "oper",
            "date": "2020-01-01",
            "levtype": "sfc",
            "step": 0,
            "param": 167,
            "time": 0,
            "klasse": "ea",  # 'klasse' is not a valid MARS key
        },
        [AxisDefinition(["param"], Chunking.SINGLE_VALUE)],
        ExtractorType.GRIB,
    )
    with pytest.raises(MarsRequestFormattingError, match="Did you misspell a MARS key"):
        builder.build()


def test_invalid_chunking_type_raises() -> None:
    """Passing an arbitrary object as the chunking argument raises InternalError
    immediately when constructing AxisDefinition, before any builder or FDB call.
    """
    with pytest.raises(InternalError):
        AxisDefinition(["param"], object())
