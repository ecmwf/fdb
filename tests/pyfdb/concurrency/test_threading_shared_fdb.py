# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""Thread-safety tests where all workers share a single FDB instance.

A single FDB object is created before the threads start. Workers synchronise at
a barrier and then all call the same method simultaneously. This verifies that
the FDB C++ implementation and the pybind11 bindings tolerate concurrent access
to the same underlying object.

Some tests are marked xfail because of known C++ data races inside the FDB
library itself (tracked as FDB-695, FDB-696, FDB-703). Remove the markers once
the corresponding tickets are resolved.
"""
"""
This comment is an overview what would need to be fixed for enabling multi-threading in a shared FDB
instance:

┌──────────────┬───────────────────────────┬────────────────────────────────┬──────────────────────────────────────────────────────────────────┬─────────┐
│  Operation   │           Race            │        Offending member        │                      Call chain to the race                      │ Ticket  │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│              │ Unguarded std::unique_ptr │                                │ FDB::archive() → LocalFDB::archive() → if (!archiver_) @         │         │
│ archive()    │  check-then-act on first  │ LocalFDB::archiver_            │ LocalFDB.cc:47                                                   │ FDB-695 │
│              │ call                      │                                │                                                                  │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│ archive()    │ Plain bool written        │ FDB::dirty_                    │ FDB::archive() → dirty_ = true @ FDB.cc:138                      │ FDB-696 │
│              │ without synchronisation   │                                │                                                                  │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│              │ Plain bool read and       │                                │                                                                  │         │
│ flush()      │ cleared without           │ FDB::dirty_                    │ FDB::flush() → if (dirty_) + dirty_ = false @ FDB.cc:314         │ FDB-696 │
│              │ synchronisation           │                                │                                                                  │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│ dirty()      │ Plain bool read while     │ FDB::dirty_                    │ FDB::dirty() reads dirty_ @ FDB.h:304 while archive() writes it  │ FDB-696 │
│              │ archive() may write       │                                │                                                                  │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│ retrieve() / │ Unguarded std::unique_ptr │                                │ FDB::retrieve() → FDB::inspect() → LocalFDB::inspect() → if      │         │
│  inspect()   │  check-then-act on first  │ LocalFDB::inspector_           │ (!inspector_) @ LocalFDB.cc:66                                   │ FDB-695 │
│              │ call                      │                                │                                                                  │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│              │                           │                                │ LocalFDB::inspect() → queryInternal<InspectVisitor>() →          │         │
│ retrieve() / │ Plain bool lazy-init on   │ Config::schemaPathInitialised_ │ background thread → EntryVisitMechanism::visit() @               │ FDB-703 │
│  inspect()   │ shared Config, no lock    │                                │ EntryVisitMechanism.cc:125 → dbConfig_.schema() →                │         │
│              │                           │                                │ initializeSchemaPath() @ Config.cc:177                           │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│ list()       │ Plain bool lazy-init on   │ Config::schemaPathInitialised_ │ LocalFDB::list() → queryInternal<ListVisitor>() → background     │ FDB-703 │
│              │ shared Config, no lock    │                                │ thread → same initializeSchemaPath() path                        │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│ axes()       │ Plain bool lazy-init on   │ Config::schemaPathInitialised_ │ FDB::axes() → FDB::axesIterator() → queryInternal<AxesVisitor>() │ FDB-703 │
│              │ shared Config, no lock    │                                │  → background thread → same initializeSchemaPath() path          │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│ stats()      │ Plain bool lazy-init on   │ Config::schemaPathInitialised_ │ FDB::stats() → LocalFDB::stats() → queryInternal<StatsVisitor>() │ FDB-703 │
│              │ shared Config, no lock    │                                │  → background thread → same initializeSchemaPath() path          │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│ wipe()       │ Plain bool lazy-init on   │ Config::schemaPathInitialised_ │ FDB::wipe() → LocalFDB::wipe() → queryInternal<WipeVisitor>() →  │ FDB-703 │
│              │ shared Config, no lock    │                                │ background thread → same initializeSchemaPath() path             │         │
├──────────────┼───────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────────┼─────────┤
│ All ops      │ Unguarded                 │                                │ Calling thread emplaces in forwardApiCall() @ RemoteFDB.cc:255;  │         │
│ (remote      │ std::unordered_map        │ RemoteFDB::messageQueues_      │ background listener erases in handle() @ RemoteFDB.cc:318-378;   │ FDB-702 │
│ config)      │ mutated from two threads  │                                │ no lock between them                                             │         │
└──────────────┴───────────────────────────┴────────────────────────────────────────────┴──────────────────────────────────────────────────────┴─────────┘
"""


import threading

import pytest

from pyfdb import FDB, ControlAction, ControlIdentifier

_SELECTION = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "levtype": "sfc",
    "step": "0",
    "time": "1800",
}

_DATE_SELECTION = {**_SELECTION, "date": "20200101"}
_RETRIEVE_SELECTION = {**_SELECTION, "param": ["167", "131", "132"], "date": "20200101"}
_INSPECT_SELECTION = {**_SELECTION, "param": "131", "date": "20200101"}

_ARCHIVE_SELECTION = {
    "class": "rd",
    "expver": "xxxx",
    "stream": "oper",
    "date": "20191110",
    "time": "0000",
    "domain": "g",
    "type": "an",
    "levtype": "pl",
    "step": "0",
}
_ARCHIVE_LEVELS = ["300", "500", "850", "1000"]

NUM_THREADS = 24
TIMEOUT = 30  # seconds — a join that outlasts this almost certainly hit a deadlock


def _run_threads(workers):
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=TIMEOUT)


def _assert_completed(threads, errors):
    assert not any(t.is_alive() for t in threads), "Thread(s) still alive — possible deadlock"
    assert not errors, f"Exception(s) raised in workers: {errors}"


def test_concurrent_list(read_only_fdb_setup):
    """All threads call list() on a shared FDB simultaneously; results must agree."""
    results = [None] * NUM_THREADS
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            barrier.wait(timeout=TIMEOUT)
            results[idx] = len(list(fdb.list(_SELECTION)))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r == results[0] for r in results), "Inconsistent list counts across threads"


def test_concurrent_retrieve(read_only_fdb_setup):
    """All threads call retrieve() on a shared FDB simultaneously; each must read valid GRIB data."""
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            barrier.wait(timeout=TIMEOUT)
            with fdb.retrieve(_RETRIEVE_SELECTION) as dh:
                assert dh.read(4) == b"GRIB"
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_inspect(read_only_fdb_setup):
    """All threads call inspect() on a shared FDB simultaneously; counts must agree."""
    results = [None] * NUM_THREADS
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            barrier.wait(timeout=TIMEOUT)
            results[idx] = len(list(fdb.inspect(_INSPECT_SELECTION)))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r == results[0] for r in results), "Inconsistent inspect counts across threads"
    assert results[0] is not None and results[0] > 0, "inspect returned no results"


def test_concurrent_flush(empty_fdb_setup):
    """All threads call flush() on a shared FDB simultaneously; must complete without deadlock."""
    errors = []
    fdb = FDB(empty_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            barrier.wait(timeout=TIMEOUT)
            fdb.flush()
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_status(read_only_fdb_setup):
    """All threads call status() on a shared FDB simultaneously; must complete without error."""
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            barrier.wait(timeout=TIMEOUT)
            list(fdb.status(_SELECTION))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


@pytest.mark.xfail(
    strict=False,
    reason=(
        "Config::initializeSchemaPath() has an unguarded lazy-initialisation race on "
        "schemaPathInitialised_ (FDB-703). stats() calls queryInternal on a shared Config "
        "object, racing on the first concurrent call. Remove once FDB-703 is fixed."
    ),
)
def test_concurrent_stats(read_only_fdb_setup):
    """All threads call stats() on a shared FDB simultaneously; all must return non-empty results."""
    results = [None] * NUM_THREADS
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            barrier.wait(timeout=TIMEOUT)
            results[idx] = list(fdb.stats(_DATE_SELECTION))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r is not None and len(r) > 0 for r in results), "stats returned no results"


@pytest.mark.xfail(
    strict=False,
    reason=(
        "Config::initializeSchemaPath() has an unguarded lazy-initialisation race on "
        "schemaPathInitialised_ (FDB-703). axes() calls queryInternal on a shared Config "
        "object, racing on the first concurrent call. Remove once FDB-703 is fixed."
    ),
)
def test_concurrent_axes(read_only_fdb_setup):
    """All threads call axes() on a shared FDB simultaneously; key sets must agree."""
    results = [None] * NUM_THREADS
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            barrier.wait(timeout=TIMEOUT)
            results[idx] = set(fdb.axes(_SELECTION).keys())
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r == results[0] for r in results), "Inconsistent axes keys across threads"


@pytest.mark.xfail(
    strict=False,
    reason=(
        "Config::initializeSchemaPath() has an unguarded lazy-initialisation race on "
        "schemaPathInitialised_ (FDB-703). wipe() calls queryInternal on a shared Config "
        "object, racing on the first concurrent call. Remove once FDB-703 is fixed."
    ),
)
def test_concurrent_wipe(read_only_fdb_setup):
    """All threads call wipe(doit=False) on a shared FDB simultaneously; dry-run only."""
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            barrier.wait(timeout=TIMEOUT)
            list(fdb.wipe(_DATE_SELECTION, doit=False))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


@pytest.mark.xfail(
    strict=False,
    reason=(
        "Config::initializeSchemaPath() has an unguarded lazy-initialisation race on "
        "schemaPathInitialised_ (FDB-703). purge() calls queryInternal on a shared Config "
        "object, racing on the first concurrent call. Remove once FDB-703 is fixed."
    ),
)
def test_concurrent_purge(read_only_fdb_setup):
    """All threads call purge(doit=False) on a shared FDB simultaneously; dry-run only."""
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            barrier.wait(timeout=TIMEOUT)
            list(fdb.purge(_DATE_SELECTION, doit=False))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_control(read_only_fdb_setup):
    """All threads call control(ENABLE RETRIEVE) on a shared FDB simultaneously; idempotent, must not error."""
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            barrier.wait(timeout=TIMEOUT)
            list(fdb.control(_SELECTION, ControlAction.ENABLE, [ControlIdentifier.RETRIEVE]))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_enabled(read_only_fdb_setup):
    """All threads call enabled(RETRIEVE) on a shared FDB simultaneously; all must return True."""
    results = [None] * NUM_THREADS
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            barrier.wait(timeout=TIMEOUT)
            results[idx] = fdb.enabled(ControlIdentifier.RETRIEVE)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r is True for r in results), "Not all threads saw RETRIEVE as enabled"


def test_concurrent_config(read_only_fdb_setup):
    """All threads call config() on a shared FDB simultaneously; all must return a non-empty system config."""
    results = [None] * NUM_THREADS
    errors = []
    fdb = FDB(read_only_fdb_setup)
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            barrier.wait(timeout=TIMEOUT)
            system_config, _ = fdb.config()
            results[idx] = system_config
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r is not None for r in results), "config() returned None for some threads"


@pytest.mark.xfail(
    strict=False,
    reason=(
        "LocalFDB::archive() has an unguarded lazy-initialisation race on archiver_ "
        "(FDB-695). The barrier forces all threads to hit archive() simultaneously, "
        "maximising the chance of triggering the race window. Remove once FDB-695 is fixed."
    ),
)
def test_concurrent_archive(empty_fdb_setup):
    """All threads call archive() on a shared FDB concurrently; all distinct fields must be retrievable."""
    errors = []
    payloads = {lvl: f"data-for-level-{lvl}".encode() for lvl in _ARCHIVE_LEVELS}
    fdb = FDB(empty_fdb_setup)
    barrier = threading.Barrier(len(_ARCHIVE_LEVELS))

    def worker(level):
        try:
            barrier.wait(timeout=TIMEOUT)
            fdb.archive(
                data=payloads[level],
                identifier={**_ARCHIVE_SELECTION, "levelist": level, "param": "138"},
            )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(lvl,)) for lvl in _ARCHIVE_LEVELS]
    _run_threads(threads)
    _assert_completed(threads, errors)

    fdb.flush()

    for level in _ARCHIVE_LEVELS:
        with fdb.retrieve({**_ARCHIVE_SELECTION, "levelist": level, "param": "138"}) as dh:
            assert dh.readall() == payloads[level], f"Data mismatch for levelist={level}"


def test_concurrent_dirty(empty_fdb_setup):
    """Concurrent dirty() reads on a shared FDB: archive once, verify all threads see dirty=True,
    flush, then verify all threads see dirty=False.

    Archive and dirty-check are separated (serial archive, then concurrent reads) to avoid
    the FDB-696 write-read race on dirty_.
    """
    results_before = [None] * NUM_THREADS
    results_after = [None] * NUM_THREADS
    errors = []

    fdb = FDB(empty_fdb_setup)
    fdb.archive(
        data=b"dirty-test-data",
        identifier={**_ARCHIVE_SELECTION, "levelist": "300", "param": "138"},
    )

    barrier1 = threading.Barrier(NUM_THREADS)

    def worker_before(idx):
        try:
            barrier1.wait(timeout=TIMEOUT)
            results_before[idx] = fdb.dirty()
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker_before, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r is True for r in results_before), "Expected dirty=True before flush"

    fdb.flush()
    errors.clear()

    barrier2 = threading.Barrier(NUM_THREADS)

    def worker_after(idx):
        try:
            barrier2.wait(timeout=TIMEOUT)
            results_after[idx] = fdb.dirty()
        except Exception as exc:
            errors.append(exc)

    threads2 = [threading.Thread(target=worker_after, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads2)
    _assert_completed(threads2, errors)
    assert all(r is False for r in results_after), "Expected dirty=False after flush"


def test_python_thread_runs_during_fdb(read_only_fdb_setup):
    """A pure-Python thread must make progress while an FDB thread holds C++ calls.

    The GIL must be released during blocking FDB operations so that concurrent
    Python threads — such as a port-forwarding thread in remote mode — are not
    starved. A counter thread increments a value in a tight loop while an FDB
    thread repeatedly calls list(); a zero counter means the GIL was never freed.
    """
    fdb_done = threading.Event()
    counter = [0]
    errors = []
    fdb = FDB(read_only_fdb_setup)

    def fdb_worker():
        try:
            for _ in range(50):
                list(fdb.list(_SELECTION))
        except Exception as exc:
            errors.append(exc)
        finally:
            fdb_done.set()

    def counter_worker():
        while not fdb_done.wait(timeout=0.001):
            counter[0] += 1

    fdb_thread = threading.Thread(target=fdb_worker)
    counter_thread = threading.Thread(target=counter_worker, daemon=True)

    counter_thread.start()
    fdb_thread.start()
    fdb_thread.join(timeout=TIMEOUT)
    counter_thread.join(timeout=5)

    assert not fdb_thread.is_alive(), f"FDB thread still alive after {TIMEOUT}s — possible deadlock"
    assert not counter_thread.is_alive(), "Counter thread did not stop"
    assert not errors, f"Exception(s) in FDB thread: {errors}"
    assert counter[0] > 0, (
        "Counter thread made no progress while FDB C++ calls were in flight "
        ", so the GIL may not have been released"
    )


def test_workflow(empty_fdb_setup):
    """All workers archive concurrently on a shared FDB, then flush, list, inspect,
    archive one more field, list again, and retrieve — all through the same FDB instance.
    """
    errors = []
    payloads = {lvl: f"workflow-payload-{lvl}".encode() for lvl in _ARCHIVE_LEVELS}
    fdb = FDB(empty_fdb_setup)
    barrier = threading.Barrier(len(_ARCHIVE_LEVELS))

    def archive_worker(level):
        try:
            barrier.wait(timeout=TIMEOUT)
            fdb.archive(
                data=payloads[level],
                identifier={**_ARCHIVE_SELECTION, "levelist": level, "param": "138"},
            )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=archive_worker, args=(lvl,)) for lvl in _ARCHIVE_LEVELS]
    _run_threads(threads)
    _assert_completed(threads, errors)

    fdb.flush()
    arch_selection = {**_ARCHIVE_SELECTION, "param": "138"}

    listed = list(fdb.list(arch_selection))
    assert len(listed) == len(_ARCHIVE_LEVELS), (
        f"Expected {len(_ARCHIVE_LEVELS)} fields after phase-1 archive, got {len(listed)}"
    )

    inspected = list(fdb.inspect({**arch_selection, "levelist": "/".join(_ARCHIVE_LEVELS)}))
    assert len(inspected) == len(_ARCHIVE_LEVELS), (
        f"Expected {len(_ARCHIVE_LEVELS)} fields from inspect, got {len(inspected)}"
    )

    # Open a fresh FDB so the inspector initialises against the complete 5-field TOC.
    # The prior inspect() call cached a 4-field snapshot; retrieve() routes through that
    # cached inspector and would return empty bytes for the newly added field.
    fdb = FDB(empty_fdb_setup)
    extra_level = "200"
    extra_payload = b"extra-workflow-field"
    fdb.archive(
        data=extra_payload,
        identifier={**_ARCHIVE_SELECTION, "levelist": extra_level, "param": "138"},
    )
    fdb.flush()

    listed_after = list(fdb.list(arch_selection))
    assert len(listed_after) == len(_ARCHIVE_LEVELS) + 1, (
        f"Expected {len(_ARCHIVE_LEVELS) + 1} fields after extra archive, got {len(listed_after)}"
    )

    with fdb.retrieve({**_ARCHIVE_SELECTION, "levelist": extra_level, "param": "138"}) as dh:
        assert dh.readall() == extra_payload, "Retrieved bytes do not match archived payload"
