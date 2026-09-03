# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""
End-to-end test: pyfdb through a Python port-forwarding proxy against a real
remote FDB (separate catalogue + store servers).

Verifies that py::call_guard<py::gil_scoped_release>() is applied to all
blocking C++ FDB calls so that the pure-Python proxy relay threads can run
concurrently: pyfdb -> Python proxy -> catalogue-server -> store-server.
"""

import logging
import socket
import threading

import pyfdb

log = logging.getLogger(__name__)


class Proxy:
    def __init__(
        self,
        upstream_port: int,
        upstream_host: str = "127.0.0.1",
        proxy_host: str = "127.0.0.1",
        proxy_port: int = 0,
    ):
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.proxy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.proxy_sock.bind((proxy_host, proxy_port))
        self.proxy_sock.listen()
        self.proxy_sock.settimeout(1.0)
        self.proxy_port = self.proxy_sock.getsockname()[1]
        self.proxy_stop = threading.Event()
        log.debug(
            "proxy listen on port %d -> upstream port %d",
            self.proxy_port,
            self.upstream_port,
        )

    def start(self):
        self.proxy_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.proxy_thread.start()

    def _forward(self, source: socket.socket, destination: socket.socket) -> None:
        src = source.getpeername()
        dst = destination.getpeername()
        log.debug("proxy: relay started %s -> %s", src, dst)
        bytes_forwarded = 0
        try:
            while not self.proxy_stop.is_set():
                try:
                    data = source.recv(4096)
                except OSError:
                    break
                if not data:
                    break
                destination.sendall(data)
                bytes_forwarded += len(data)
        finally:
            log.debug("proxy: relay done %s -> %s (%d bytes)", src, dst, bytes_forwarded)
            try:
                destination.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            for sock in (destination, source):
                try:
                    sock.close()
                except OSError:
                    pass

    def _accept_loop(self):
        log.debug("proxy: accept loop started")
        while not self.proxy_stop.is_set():
            try:
                client, addr = self.proxy_sock.accept()
                log.debug("proxy: accepted connection from %s", addr)
            except TimeoutError:
                continue  # 1-s poll — re-check proxy_stop
            except OSError:
                break  # socket closed by stop()
            # A single pyfdb.FDB instance maintains one persistent TCP connection,
            # so this block runs exactly once per test.
            upstream = socket.create_connection(("127.0.0.1", self.upstream_port))
            log.debug("proxy: opened upstream connection to port %d", self.upstream_port)
            self.upstream_thread = threading.Thread(target=self._forward, args=(client, upstream), daemon=True)
            self.client_thread = threading.Thread(target=self._forward, args=(upstream, client), daemon=True)
            self.upstream_thread.start()
            self.client_thread.start()
        log.debug("proxy: accept loop stopped")

    def port(self):
        return self.proxy_port

    def stop(self):
        self.proxy_stop.set()
        self.proxy_sock.close()
        self.proxy_thread.join(timeout=5)
        log.debug("proxy thread stopped")
        for name in ("client_thread", "upstream_thread"):
            t = getattr(self, name, None)
            if t is not None:
                t.join(timeout=5)
                log.debug("%s stopped", name)


# Keys that match the x138-300.grib fixture archived by read_only_store_setup
_SELECTION = {
    "class": "rd",
    "expver": "xxxx",
    "stream": "oper",
    "date": "20191110",
    "time": "0000",
    "domain": "g",
    "type": "an",
    "levtype": "pl",
    "step": "0",
    "levelist": "300",
    "param": "138",
}

DEADLOCK_TIMEOUT = 10  # seconds


def test_remote_fdb_threading_proxy(fdb_servers, data_path):
    """List and retrieve through a Python proxy; hangs if the GIL is held during C++ FDB calls."""
    catalogue_port = fdb_servers["catalogue_port"]
    log.debug("catalogue server on port %d", catalogue_port)

    proxy = Proxy(catalogue_port)
    proxy.start()

    NUM_WORKERS = 20
    log.debug("starting %d FDB worker thread(s)", NUM_WORKERS)

    errors = []
    results = [None] * NUM_WORKERS

    # The context manager ensures C++ connection teardown (FDB.close()) runs before
    # proxy.stop() so the proxy relay threads can complete the TCP handshake cleanly.
    with pyfdb.FDB(config={"type": "remote", "host": "localhost", "port": proxy.port()}) as fdb:
        log.debug("FDB instance created via proxy port %d", proxy.port())

        def fdb_worker(idx):
            log.debug("worker %d: starting", idx)
            try:
                log.debug("worker %d: acquired lock, running list()", idx)
                list_count = len(list(fdb.list({})))
                log.debug(
                    "worker %d: list() returned %d item(s), running retrieve()",
                    idx,
                    list_count,
                )
                with fdb.retrieve(_SELECTION) as dh:
                    retrieved = dh.readall()
                log.debug("worker %d: retrieve() returned %d bytes", idx, len(retrieved))
                results[idx] = {"list_count": list_count, "retrieved": retrieved}
            except Exception as exc:
                log.debug("worker %d: raised %r", idx, exc)
                errors.append(exc)
            log.debug("worker %d: done", idx)

        workers = [
            threading.Thread(target=fdb_worker, args=(i,), name=f"fdb-worker-{i}", daemon=True)
            for i in range(NUM_WORKERS)
        ]
        for w in workers:
            w.start()
        for w in workers:
            w.join(timeout=DEADLOCK_TIMEOUT)

    proxy.stop()

    assert not any(w.is_alive() for w in workers), (
        f"One or more FDB worker threads still alive after {DEADLOCK_TIMEOUT}s — "
        "the GIL was likely not released during the C++ FDB call, causing a deadlock"
    )
    assert not errors, f"FDB worker(s) raised: {errors}"

    expected = (data_path / "x138-300.grib").read_bytes()
    log.debug("expected payload size: %d bytes", len(expected))
    for idx, result in enumerate(results):
        assert result is not None, f"Worker {idx} produced no result"
        assert result["list_count"] == 1, f"Worker {idx}: expected 1 listed field, got {result['list_count']}"
        assert result["retrieved"] == expected, f"Worker {idx}: retrieved data does not match x138-300.grib"
    log.debug("all %d worker(s) passed", NUM_WORKERS)


def test_remote_fdb_threading_proxy_partial_read(fdb_servers):
    """
    Retrieve through a Python proxy but only partially consume the DataHandle before
    closing it; hangs if the GIL is held during DataHandle.close().

    When a reader abandons a retrieve mid-stream the C++ close() must flush and
    tear down the underlying TCP stream.  The proxy relay threads must be able to
    run while that teardown is in progress — they can only do so if the GIL is
    released during the C++ DataHandle.close() call.
    """
    catalogue_port = fdb_servers["catalogue_port"]
    log.debug("catalogue server on port %d", catalogue_port)

    proxy = Proxy(catalogue_port)
    proxy.start()

    NUM_WORKERS = 20
    log.debug("starting %d FDB worker thread(s)", NUM_WORKERS)

    errors = []
    results = [None] * NUM_WORKERS

    with pyfdb.FDB(config={"type": "remote", "host": "localhost", "port": proxy.port()}) as fdb:
        log.debug("FDB instance created via proxy port %d", proxy.port())

        def fdb_worker(idx):
            log.debug("worker %d: starting", idx)
            try:
                log.debug("worker %d: acquired lock, running retrieve()", idx)
                with fdb.retrieve(_SELECTION) as dh:
                    # Read only the GRIB magic bytes — intentionally abandon
                    # the rest of the stream to exercise partial-read close.
                    magic = dh.read(4)
                    assert magic == b"GRIB"
                log.debug("worker %d: partial read returned %r, handle closed", idx, magic)
                results[idx] = magic
            except Exception as exc:
                log.debug("worker %d: raised %r", idx, exc)
                errors.append(exc)
            log.debug("worker %d: done", idx)

        workers = [
            threading.Thread(target=fdb_worker, args=(i,), name=f"fdb-worker-{i}", daemon=True)
            for i in range(NUM_WORKERS)
        ]
        for w in workers:
            w.start()
        for w in workers:
            w.join(timeout=DEADLOCK_TIMEOUT)

    proxy.stop()

    assert not any(w.is_alive() for w in workers), (
        f"One or more FDB worker threads still alive after {DEADLOCK_TIMEOUT}s — "
        "the GIL was likely not released during DataHandle.close(), causing a deadlock"
    )
    assert not errors, f"FDB worker(s) raised: {errors}"

    for idx, result in enumerate(results):
        assert result == b"GRIB", f"Worker {idx}: expected GRIB magic bytes, got {result!r}"
    log.debug("all %d worker(s) passed", NUM_WORKERS)
