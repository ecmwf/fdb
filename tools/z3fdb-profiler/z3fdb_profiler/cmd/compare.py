# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
import logging
from itertools import product
from pathlib import Path

import numpy
import zarr

from z3fdb_profiler.decorators import profile
from z3fdb_profiler.z3fdb import make_store

logger = logging.getLogger(__name__)


def register_cmd(subparsers):
    parser = subparsers.add_parser(
        "compare", help="Compare z3fdb view with on disk zarr array"
    )
    parser.set_defaults(func=execute_cmd)
    parser.add_argument(
        "-z",
        "--zarr",
        help="Path where zarr store will be read from.",
        type=Path,
        default=Path.cwd() / "zarr-export",
    )
    parser.add_argument(
        "--fdb-config",
        help="Path to fdb config.yaml. If not set FDB_HOME will be used",
        type=Path,
    )
    parser.add_argument("--view", help="View file to load", type=Path, required=True)


@profile("compare")
def execute_cmd(args):
    print("Comparing view with native zarr array")
    view_data = zarr.open_array(
        make_store(args.view, args.fdb_config),
        mode="r",
        zarr_format=3,
        use_consolidated=False,
    )
    zarr_data = zarr.open_array(
        args.zarr, mode="r", zarr_format=3, use_consolidated=False
    )
    assert view_data.shape == zarr_data.shape
    for idx in product(*(range(dim) for dim in zarr_data.shape[:-1])):
        logger.debug(f"Testing {idx}")
        assert numpy.array_equal(view_data[idx], zarr_data[idx])
    print("Finished")
