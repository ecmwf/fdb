import logging
import os
import pathlib
import shutil
import site
import socket
import subprocess
import sys
import threading
import time

import pytest
import yaml

import pyfdb

logger = logging.getLogger(__name__)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="function", autouse=False)
def read_only_store_setup(data_path, session_tmp, function_tmp):
    """Pre-populate a store-server root with x138-300.grib. Returns (config_path, port)."""
    schema_path = session_tmp / "schema"
    if not schema_path.exists():
        shutil.copy(data_path / "schema", schema_path)

    data_root = function_tmp / "data_root"
    data_root.mkdir()

    local_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [{"handler": "Default", "roots": [{"path": str(data_root)}]}],
    }
    with pyfdb.FDB(config=local_config) as fdb:
        fdb.archive((data_path / "x138-300.grib").read_bytes())

    store_port = _free_port()
    store_config = {
        "type": "store",
        "serverPort": store_port,
        "spaces": [{"handler": "Default", "roots": [{"path": str(data_root)}]}],
    }
    store_config_path = function_tmp / "store_config.yaml"
    store_config_path.write_text(yaml.dump(store_config))
    logger.debug(f"Writing fdb config to path: {store_config_path}")
    return store_config_path, store_port


@pytest.fixture(scope="function", autouse=False)
def catalogue_setup(data_path, session_tmp, function_tmp, read_only_store_setup):
    """Create a catalogue-server config over the same root as read_only_store_setup. Returns (config_path, port)."""
    _, store_port = read_only_store_setup
    data_root = function_tmp / "data_root"

    catalogue_port = _free_port()
    catalogue_config = dict(
        type="catalogue",
        serverPort=catalogue_port,
        engine="toc",
        schema=str(session_tmp / "schema"),
        stores=[dict(default=f"localhost:{store_port}", serveLocalData=True)],
        spaces=[dict(handler="Default", roots=[{"path": str(data_root)}])],
    )
    catalogue_config_path = function_tmp / "catalogue_config.yaml"
    catalogue_config_path.write_text(yaml.dump(catalogue_config))
    return catalogue_config_path, catalogue_port


def _build_server_env(base_env: dict) -> dict:
    """Prepend site-packages lib dirs for eckit/metkit/eccodes/fdb5 to LD/DYLD_LIBRARY_PATH."""
    env = dict(base_env)
    for var in ("DYLD_LIBRARY_PATH", "LD_LIBRARY_PATH"):
        existing = env.get(var, "")
        env[var] = ":".join([existing] if existing else [])
    return env


def _stream_to_log(name: str, pipe) -> None:
    for raw in pipe:
        logger.debug("[%s] %s", name, raw.decode(errors="replace").rstrip())


def _find_fdb_server() -> pathlib.Path:
    for sp in site.getsitepackages():
        candidate = pathlib.Path(sp) / "fdb5lib" / "bin" / "fdb-server"
        if candidate.is_file():
            return candidate
    venv_bin = pathlib.Path(sys.prefix) / "bin" / "fdb-server"
    if venv_bin.is_file():
        return venv_bin
    on_path = shutil.which("fdb-server")
    logger.debug(f"Found FDB-SERVER on: {on_path}")
    if on_path is not None:
        return pathlib.Path(on_path)
    raise FileNotFoundError("fdb-server not found in site-packages (fdb5lib/bin/fdb-server), venv bin/, or PATH")


def _wait_for_port(port: int, timeout: float = 30.0, proc: subprocess.Popen | None = None) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            out, err = proc.communicate()
            raise RuntimeError(
                f"fdb-server exited (rc={proc.returncode}) before binding to port {port}\n"
                f"--- stderr ---\n{err.decode(errors='replace') if err else '(none)'}"
                f"--- stdout ---\n{out.decode(errors='replace') if out else '(none)'}"
            )
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise TimeoutError(f"fdb-server did not bind to port {port} within {timeout}s")


@pytest.fixture(scope="function")
def fdb_servers(read_only_store_setup, catalogue_setup):
    """Start store + catalogue servers; yield {"catalogue_port": int, "store_port": int}."""
    store_cfg_path, store_port = read_only_store_setup
    catalogue_cfg_path, catalogue_port = catalogue_setup

    server_env = _build_server_env(os.environ)
    fdb_server_bin = _find_fdb_server()

    logger.debug(f"Starting store with config at: {store_cfg_path}")
    store_proc = subprocess.Popen(
        [str(fdb_server_bin)],
        env={**server_env, "FDB_CONFIG_FILE": str(store_cfg_path)},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    store_log_thread = threading.Thread(target=_stream_to_log, args=("store", store_proc.stdout), daemon=True)
    store_log_thread.start()
    _wait_for_port(store_port, proc=store_proc)
    # Grace period: the probe connection triggers the server's unexpected-disconnect
    # handler; wait for it to settle before accepting real connections.
    time.sleep(2)

    catalogue_proc = subprocess.Popen(
        [str(fdb_server_bin)],
        env={**server_env, "FDB_CONFIG_FILE": str(catalogue_cfg_path)},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    catalogue_log_thread = threading.Thread(
        target=_stream_to_log, args=("catalogue", catalogue_proc.stdout), daemon=True
    )
    catalogue_log_thread.start()
    _wait_for_port(catalogue_port, proc=catalogue_proc)
    time.sleep(0.5)

    yield {"catalogue_port": catalogue_port, "store_port": store_port}

    catalogue_proc.terminate()
    store_proc.terminate()
    catalogue_log_thread.join(timeout=5)
    store_log_thread.join(timeout=5)
    catalogue_proc.wait(timeout=5)
    store_proc.wait(timeout=5)
