import pytest
import pathlib

import pyfdb


def create_default_fdb_at(dest_path: pathlib.Path, data_path: pathlib.Path):
    config = dict(
        type="local",
        engine="toc",
        schema=str(data_path / "default_fdb_schema"),
        spaces=[
            dict(
                handler="Default",
                roots=[
                    {"path": str(dest_path)},
                ],
            )
        ],
    )
    return dest_path, pyfdb.FDB(config)


@pytest.fixture(scope="session")
def data_path() -> pathlib.Path:
    """
    Provides path to test data
    """
    path = pathlib.Path(__file__).resolve().parent / ".." / "data"
    assert path.exists()
    return path


@pytest.fixture
def setup_fdb_tmp_dir(tmp_path, data_path):
    # Return a function to randomize on each call
    return lambda : create_default_fdb_at(tmp_path, data_path)
