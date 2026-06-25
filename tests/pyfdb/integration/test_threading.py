# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""
Tests for thread-safe use of pyfdb.

Background
----------
When a Python thread calls into a pyfdb C++ method the pybind11 layer acquires
the GIL for the duration of that call by default. If a second Python thread is
required to make forward progress (e.g. a port-forwarding thread shuttling bytes
between the caller and a remote FDB), that thread will be blocked waiting for
the GIL, causing a deadlock: the FDB call waits for data that the
port-forwarding thread cannot deliver because it cannot run.

The fix is to release the GIL inside every pybind11 binding that delegates to a
potentially-blocking C++ FDB call (archive, flush, retrieve, list, inspect,
status, wipe, purge, stats, control, axes, enabled, dirty, config, DataHandle
open/close/read/size, and all iterator __next__ methods).
"""

import threading

import pytest

from pyfdb import FDB


SELECTION = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "levtype": "sfc",
    "step": "0",
    "time": "1800",
}

DEADLOCK_TIMEOUT = 30  # seconds — failing this almost certainly means a deadlock


def test_concurrent_list_operations(read_only_fdb_setup):
    """Multiple threads calling fdb.list() concurrently must all complete correctly."""
    NUM_THREADS = 4
    results = [None] * NUM_THREADS
    errors = []

    def worker(idx):
        try:
            fdb = FDB(read_only_fdb_setup)
            results[idx] = len(list(fdb.list(SELECTION)))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=DEADLOCK_TIMEOUT)

    assert not any(t.is_alive() for t in threads), (
        "Thread(s) still alive — possible deadlock"
    )
    assert not errors, f"Exception(s) raised in worker threads: {errors}"
    assert all(r == results[0] for r in results), (
        "Threads returned inconsistent list counts"
    )


def test_concurrent_retrieve_operations(read_only_fdb_setup):
    """Multiple threads calling fdb.retrieve() and reading data concurrently."""
    NUM_THREADS = 4
    errors = []

    selection = {**SELECTION, "param": ["167", "131", "132"], "date": "20200101"}

    def worker():
        try:
            fdb = FDB(read_only_fdb_setup)
            with fdb.retrieve(selection) as dh:
                magic = dh.read(4)
            assert magic == b"GRIB"
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=DEADLOCK_TIMEOUT)

    assert not any(t.is_alive() for t in threads), (
        "Thread(s) still alive — possible deadlock"
    )
    assert not errors, f"Exception(s) raised in worker threads: {errors}"


def test_python_thread_runs_during_fdb_operations(read_only_fdb_setup):
    """
    The GIL must be released during blocking FDB C++ calls so that a concurrent
    Python thread (e.g. a port-forwarding thread) can run while the FDB call is
    in progress.

    This test simulates that scenario: an FDB worker thread loops over many list
    operations while a second Python thread increments a counter.  If the GIL
    were held throughout every C++ call, the counter thread could only run between
    individual C++ calls (at CPython's GIL check interval), and in a real remote
    scenario it could not unblock the FDB call at all.
    """
    fdb_done = threading.Event()
    counter = [0]
    errors = []

    def fdb_worker():
        try:
            fdb = FDB(read_only_fdb_setup)
            for _ in range(50):
                list(fdb.list(SELECTION))
        except Exception as exc:
            errors.append(exc)
        finally:
            fdb_done.set()

    def python_worker():
        # Pure Python work — requires the GIL on every iteration.
        # Simulates a port-forwarding thread that must keep running while
        # the FDB worker is blocked inside a C++ call.
        while not fdb_done.is_set():
            counter[0] += 1

    fdb_thread = threading.Thread(target=fdb_worker)
    python_thread = threading.Thread(target=python_worker, daemon=True)

    python_thread.start()
    fdb_thread.start()

    fdb_thread.join(timeout=DEADLOCK_TIMEOUT)
    python_thread.join(timeout=5)

    assert not fdb_thread.is_alive(), (
        f"FDB thread still alive after {DEADLOCK_TIMEOUT}s — possible deadlock"
    )
    assert not python_thread.is_alive(), "Counter thread did not stop"
    assert not errors, f"Exception(s) raised in FDB thread: {errors}"
    assert counter[0] > 0, (
        "Counter thread made no progress while FDB thread was running — "
        "the GIL may not have been released during FDB C++ calls"
    )
