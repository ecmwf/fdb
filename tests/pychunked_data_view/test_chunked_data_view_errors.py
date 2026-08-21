# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import pytest

from pychunked_data_view import (
    AxisDefinition,
    ChunkedDataViewBuilder,
    Chunking,
    ExtractorType,
)
from pychunked_data_view.exceptions import MarsRequestFormattingError

pytestmark = pytest.mark.offline


@pytest.mark.parametrize(
    "malformed_request, expected_hint",
    [
        # Trailing comma — triggers StreamParser::next in the eckit MARS parser
        ("class=ea,", "Did the MARS request end in a comma"),
        # Missing comma between key=value pairs — triggers MarsParser::parseVerb
        ("class=eadomain=g", "Did you miss a comma between keys"),
    ],
)
def test_malformed_mars_string_raises(read_only_fdb_setup, malformed_request, expected_hint) -> None:
    """Malformed raw MARS request strings are caught in build() and re-raised as
    MarsRequestFormattingError with an actionable hint.

    The Python add_part() API only accepts dicts and cannot produce such strings;
    we bypass it here by calling the underlying C++ builder directly so that the
    error-detection logic in build() can be exercised.
    """
    builder = ChunkedDataViewBuilder(read_only_fdb_setup)
    # Inject the malformed MARS string at the bindings level — addPart() stores it
    # without parsing, so no error fires here; it fires in build().
    builder._obj.add_part(malformed_request, [], ExtractorType.Grib()._obj)
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
        ExtractorType.Grib(),
    )
    with pytest.raises(MarsRequestFormattingError, match="Did you misspell a MARS key"):
        builder.build()


def test_invalid_chunking_type_raises() -> None:
    """Passing an arbitrary object as the chunking argument raises TypeError with a
    descriptive message immediately when constructing AxisDefinition, before any
    builder or FDB call.
    """
    with pytest.raises(TypeError, match="chunking must be Chunking"):
        AxisDefinition(["param"], object())


@pytest.mark.parametrize("chunk_shape", [0], ids=["zero"])
def test_invalid_fixed_chunk_size_rejected_on_construction(chunk_shape) -> None:
    """A non-positive chunk size is refused by FixedSizeChunking itself.

    AxisDefinition::FixedSizeChunking asserts chunkSize > 0 in its constructor, so this fails
    as soon as the chunking object is built — before any AxisDefinition, builder, request or
    FDB is involved. Placed here rather than with the extractor tests because it is a property
    of the chunking type, independent of which extractor consumes it.
    """
    with pytest.raises(Exception, match="The supplied chunk shape needs to be positive"):
        Chunking.FixedSizeChunk(chunk_shape=chunk_shape)
