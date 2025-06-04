import pytest
import pathlib

import pyfdb


@pytest.fixture(scope="session")
def data_path() -> pathlib.Path:
    """
    Provides path to test data
    """
    path = pathlib.Path(__file__).resolve().parent / "data"
    assert path.exists()
    return path


@pytest.fixture
def setup_fdb_tmp_dir(tmp_path_factory, data_path):
    def create_randomized_fdb_path():
        tmp_dir = tmp_path_factory.mktemp("root")
        config = dict(
            type="local",
            engine="toc",
            schema=str(data_path / "default_fdb_schema"),
            spaces=[
                dict(
                    handler="Default",
                    roots=[
                        {"path": str(tmp_dir)},
                    ],
                )
            ],
        )
        return str(tmp_dir), pyfdb.FDB(config)

    # Return a function to randomize on each call
    return create_randomized_fdb_path
