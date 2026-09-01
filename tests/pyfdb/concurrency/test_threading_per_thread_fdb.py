# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""Thread-safety tests where each worker owns a private FDB instance.

Workers construct their FDB objects concurrently, then synchronise at a barrier
before firing the operation under test simultaneously. This maximises the chance
of exposing races in the pybind11 GIL-release paths and in C++ global state
that is lazily initialised on first use.
"""

import threading

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
    """Each thread constructs its own FDB and calls list(); results must agree."""
    results = [None] * NUM_THREADS
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            results[idx] = len(list(fdb.list(_SELECTION)))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r == results[0] for r in results), "Inconsistent list counts across threads"


def test_concurrent_retrieve(read_only_fdb_setup):
    """Each thread constructs its own FDB and retrieves data; every response must be valid GRIB."""
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            with fdb.retrieve(_RETRIEVE_SELECTION) as dh:
                assert dh.read(4) == b"GRIB"
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_inspect(read_only_fdb_setup):
    """Each thread constructs its own FDB and calls inspect(); counts must agree."""
    results = [None] * NUM_THREADS
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            results[idx] = len(list(fdb.inspect(_INSPECT_SELECTION)))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r == results[0] for r in results), "Inconsistent inspect counts across threads"
    assert results[0] is not None and results[0] > 0, "inspect returned no results"


def test_concurrent_flush(read_only_fdb_setup):
    """Each thread constructs its own FDB and calls flush(); must complete without deadlock."""
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            fdb.flush()
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_status(read_only_fdb_setup):
    """Each thread constructs its own FDB and calls status(); must complete without error."""
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            list(fdb.status(_SELECTION))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_stats(read_only_fdb_setup):
    """Each thread constructs its own FDB and calls stats(); all must return non-empty results."""
    results = [None] * NUM_THREADS
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            results[idx] = list(fdb.stats(_DATE_SELECTION))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r is not None and len(r) > 0 for r in results), "stats returned no results"


def test_concurrent_axes(read_only_fdb_setup):
    """Each thread constructs its own FDB and calls axes(); key sets must agree."""
    results = [None] * NUM_THREADS
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            results[idx] = set(fdb.axes(_SELECTION).keys())
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r == results[0] for r in results), "Inconsistent axes keys across threads"


def test_concurrent_wipe(read_only_fdb_setup):
    """Each thread constructs its own FDB and calls wipe(doit=False); must complete without error."""
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            list(fdb.wipe(_DATE_SELECTION, doit=False))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_purge(read_only_fdb_setup):
    """Each thread constructs its own FDB and calls purge(doit=False); must complete without error."""
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    _SELECTION = {
        "class": "ea",
    }

    def worker():
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            list(fdb.purge(_SELECTION, doit=False))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_control(read_only_fdb_setup):
    """Each thread constructs its own FDB and re-enables RETRIEVE (idempotent); must complete without error."""
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker():
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            list(fdb.control(_SELECTION, ControlAction.ENABLE, [ControlIdentifier.RETRIEVE]))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)


def test_concurrent_enabled(read_only_fdb_setup):
    """Each thread constructs its own FDB and checks enabled(RETRIEVE); all must return True."""
    results = [None] * NUM_THREADS
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            results[idx] = fdb.enabled(ControlIdentifier.RETRIEVE)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r is True for r in results), "Not all threads saw RETRIEVE as enabled"


def test_concurrent_config(read_only_fdb_setup):
    """Each thread constructs its own FDB and calls config(); all must return a non-empty system config."""
    results = [None] * NUM_THREADS
    errors = []
    barrier = threading.Barrier(NUM_THREADS)

    def worker(idx):
        try:
            fdb = FDB(read_only_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            system_config, _ = fdb.config()
            results[idx] = system_config
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    _run_threads(threads)
    _assert_completed(threads, errors)
    assert all(r is not None for r in results), "config() returned None for some threads"


def test_concurrent_archive(empty_fdb_setup):
    """Each thread constructs its own FDB and archives a distinct field; all must be retrievable after flush."""
    errors = []
    payloads = {lvl: f"data-for-level-{lvl}".encode() for lvl in _ARCHIVE_LEVELS}
    barrier = threading.Barrier(len(_ARCHIVE_LEVELS))

    def worker(level):
        try:
            fdb = FDB(empty_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            fdb.archive(
                data=payloads[level],
                identifier={**_ARCHIVE_SELECTION, "levelist": level, "param": "138"},
            )
            fdb.flush()
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(lvl,)) for lvl in _ARCHIVE_LEVELS]
    _run_threads(threads)
    _assert_completed(threads, errors)

    fdb = FDB(empty_fdb_setup)
    for level in _ARCHIVE_LEVELS:
        with fdb.retrieve({**_ARCHIVE_SELECTION, "levelist": level, "param": "138"}) as dh:
            assert dh.readall() == payloads[level], f"Data mismatch for levelist={level}"


def test_concurrent_dirty(empty_fdb_setup):
    """Each thread archives a distinct field, checks dirty=True, flushes, then checks dirty=False."""
    errors = []
    barrier = threading.Barrier(len(_ARCHIVE_LEVELS))

    def worker(level):
        try:
            fdb = FDB(empty_fdb_setup)
            fdb.archive(
                data=f"dirty-test-{level}".encode(),
                identifier={**_ARCHIVE_SELECTION, "levelist": level, "param": "138"},
            )
            barrier.wait(timeout=TIMEOUT)
            assert fdb.dirty(), "Expected dirty=True after archive"
            fdb.flush()
            assert not fdb.dirty(), "Expected dirty=False after flush"
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(lvl,)) for lvl in _ARCHIVE_LEVELS]
    _run_threads(threads)
    _assert_completed(threads, errors)


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

    def fdb_worker():
        try:
            fdb = FDB(read_only_fdb_setup)
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
        "Counter thread made no progress while FDB C++ calls were in flight — the GIL may not have been released"
    )


def test_workflow(empty_fdb_setup):
    """Archive concurrently from multiple workers, flush, list, inspect, archive one more field,
    list again, and retrieve — verifying each step produces the expected result.

    Phase 1: each worker creates its own FDB, archives a distinct field, and flushes.
    Phase 2: a single FDB verifies counts, archives a final field, and retrieves it.
    """
    errors = []
    payloads = {lvl: f"workflow-payload-{lvl}".encode() for lvl in _ARCHIVE_LEVELS}
    barrier = threading.Barrier(len(_ARCHIVE_LEVELS))

    def archive_worker(level):
        try:
            fdb = FDB(empty_fdb_setup)
            barrier.wait(timeout=TIMEOUT)
            fdb.archive(
                data=payloads[level],
                identifier={**_ARCHIVE_SELECTION, "levelist": level, "param": "138"},
            )
            fdb.flush()
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=archive_worker, args=(lvl,)) for lvl in _ARCHIVE_LEVELS]
    _run_threads(threads)
    _assert_completed(threads, errors)

    fdb = FDB(empty_fdb_setup)
    arch_selection = {**_ARCHIVE_SELECTION, "param": "138"}

    listed = list(fdb.list(arch_selection))
    assert len(listed) == len(_ARCHIVE_LEVELS), (
        f"Expected {len(_ARCHIVE_LEVELS)} fields after phase-1 archive, got {len(listed)}"
    )

    inspected = list(fdb.inspect({**arch_selection, "levelist": "/".join(_ARCHIVE_LEVELS)}))
    assert len(inspected) == len(_ARCHIVE_LEVELS), (
        f"Expected {len(_ARCHIVE_LEVELS)} fields from inspect, got {len(inspected)}"
    )

    extra_level = "200"
    extra_payload = b"extra-workflow-field"
    fdb.archive(
        data=extra_payload,
        identifier={**_ARCHIVE_SELECTION, "levelist": extra_level, "param": "138"},
    )
    fdb.flush()

    # Open a fresh FDB so the inspector initialises against the complete 5-field TOC.
    # The prior inspect() call cached a 4-field snapshot; retrieve() routes through that
    # cached inspector and would return empty bytes for the newly added field.
    fdb = FDB(empty_fdb_setup)

    listed_after = list(fdb.list(arch_selection))
    assert len(listed_after) == len(_ARCHIVE_LEVELS) + 1, (
        f"Expected {len(_ARCHIVE_LEVELS) + 1} fields after extra archive, got {len(listed_after)}"
    )

    with fdb.retrieve({**_ARCHIVE_SELECTION, "levelist": extra_level, "param": "138"}) as dh:
        assert dh.readall() == extra_payload, "Retrieved bytes do not match archived payload"
