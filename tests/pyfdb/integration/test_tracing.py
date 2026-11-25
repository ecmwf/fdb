# SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

# The caller's W3C trace context (traceparent / tracestate keywords) reaches the FDB spans.
#
# The span exporter is process-wide: the first configuration which sets one is used. Every FDB in this module is
# configured with the same span file, and each test only looks at the spans it caused.

import json
import pathlib
import secrets
import shutil

import pytest
import yaml

from pyfdb import FDB

SELECTION = {"class": "ea", "domain": "g", "expver": "0001", "stream": "oper", "date": "20200101"}

FIELDS = {
    **SELECTION,
    "type": "an",
    "levtype": "sfc",
    "step": "0",
    "param": ["167", "165", "166"],
    "time": "1800",
}

TOP_LEVEL_SPANS = {
    "fdb.archive",
    "fdb.flush",
    "fdb.retrieve",
    "fdb.list",
    "fdb.inspect",
    "fdb.wipe",
    "fdb.purge",
}


@pytest.fixture(scope="module")
def span_file(tmp_path_factory) -> pathlib.Path:
    return tmp_path_factory.mktemp("spans") / "spans.jsonl"


def make_config(data_path, root: pathlib.Path, span_file: pathlib.Path, enabled: bool = True) -> str:
    schema = root / "schema"
    shutil.copy(data_path / "schema", schema)
    store = root / "db_store"
    store.mkdir()
    config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema),
        "spaces": [{"handler": "Default", "roots": [{"path": str(store)}]}],
        "tracing": {
            "enabled": enabled,
            "sampler": "parentbased_always_on",
            "exporter": "log",
            "path": str(span_file),
        },
    }
    return yaml.dump(config)


@pytest.fixture(scope="function")
def tracing_fdb_config(data_path, function_tmp, span_file) -> str:
    return make_config(data_path, function_tmp, span_file)


def new_traceparent() -> tuple[str, str, str]:
    trace_id = secrets.token_hex(16)
    span_id = secrets.token_hex(8)
    return f"00-{trace_id}-{span_id}-01", trace_id, span_id


def read_spans(span_file: pathlib.Path) -> list[dict]:
    if not span_file.exists():
        return []
    return [json.loads(line) for line in span_file.read_text().splitlines() if line.strip()]


def spans_of(span_file: pathlib.Path, trace_id: str) -> list[dict]:
    return [span for span in read_spans(span_file) if span["trace_id"] == trace_id]


def test_traced_calls_are_children_of_the_callers_span(tracing_fdb_config, build_grib_messages, span_file):
    traceparent, trace_id, span_id = new_traceparent()

    fdb = FDB(tracing_fdb_config)
    fdb.archive(build_grib_messages.read_bytes(), traceparent=traceparent)
    fdb.flush(traceparent=traceparent)

    assert len(list(fdb.list(SELECTION, traceparent=traceparent))) > 0
    assert len(list(fdb.inspect(FIELDS, traceparent=traceparent))) > 0

    with fdb.retrieve(FIELDS, traceparent=traceparent) as data_handle:
        assert data_handle.read(4) == b"GRIB"

    list(fdb.purge(SELECTION, traceparent=traceparent))
    list(fdb.wipe(SELECTION, traceparent=traceparent))

    spans = spans_of(span_file, trace_id)
    children = {span["name"] for span in spans if span["parent_span_id"] == span_id}
    assert TOP_LEVEL_SPANS <= children

    # Work below the API calls is part of the same trace, under their spans
    retrieve = next(span for span in spans if span["name"] == "fdb.retrieve")
    assert any(span["parent_span_id"] == retrieve["span_id"] for span in spans)


def test_tracestate_is_propagated(tracing_fdb_config, build_grib_messages, span_file):
    traceparent, trace_id, _ = new_traceparent()

    fdb = FDB(tracing_fdb_config)
    fdb.archive(build_grib_messages.read_bytes(), traceparent=traceparent, tracestate="vendor=value")
    fdb.flush(traceparent=traceparent, tracestate="vendor=value")

    spans = spans_of(span_file, trace_id)
    assert {span["name"] for span in spans} >= {"fdb.archive", "fdb.flush"}
    assert all(span.get("trace_state") == "vendor=value" for span in spans)


@pytest.mark.parametrize("traceparent", [None, ""])
def test_without_traceparent_a_new_trace_is_started(tracing_fdb_config, build_grib_messages, span_file, traceparent):
    fdb = FDB(tracing_fdb_config)
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    before = len(read_spans(span_file))
    assert len(list(fdb.list(SELECTION, traceparent=traceparent))) > 0
    new_spans = read_spans(span_file)[before:]

    roots = [span for span in new_spans if span["name"] == "fdb.list"]
    assert len(roots) == 1
    assert roots[0]["parent_span_id"] == ""


def test_malformed_traceparent_raises_value_error(tracing_fdb_config, build_grib_messages):
    fdb = FDB(tracing_fdb_config)

    with pytest.raises(ValueError, match="traceparent"):
        fdb.archive(build_grib_messages.read_bytes(), traceparent="garbage")
    with pytest.raises(ValueError, match="traceparent"):
        fdb.flush(traceparent="00-00000000000000000000000000000000-0000000000000000-01")
    with pytest.raises(ValueError, match="traceparent"):
        # generators: raised when iterated
        list(fdb.list(SELECTION, traceparent="00-xyz"))


def test_trace_context_is_keyword_only(tracing_fdb_config):
    traceparent, _, _ = new_traceparent()
    fdb = FDB(tracing_fdb_config)

    with pytest.raises(TypeError):
        fdb.flush(traceparent)


def test_tracing_disabled_in_config_records_no_span(data_path, function_tmp, build_grib_messages, span_file):
    traceparent, trace_id, _ = new_traceparent()

    fdb = FDB(make_config(data_path, function_tmp, span_file, enabled=False))
    fdb.archive(build_grib_messages.read_bytes(), traceparent=traceparent)
    fdb.flush(traceparent=traceparent)
    assert len(list(fdb.list(SELECTION, traceparent=traceparent))) > 0

    assert spans_of(span_file, trace_id) == []
