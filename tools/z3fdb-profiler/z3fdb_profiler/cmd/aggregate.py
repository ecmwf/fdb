# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
import logging
import random
from itertools import product, islice
from pathlib import Path

import numpy
import zarr

from z3fdb_profiler.decorators import profile
from z3fdb_profiler.z3fdb import make_store

logger = logging.getLogger(__name__)


def register_cmd(subparsers):
    parser = subparsers.add_parser(
        "aggregate", help="Aggregate all fields of a native zarr array"
    )
    parser.set_defaults(func=execute_cmd)
    parser.add_argument(
        "--limit",
        help="Only aggregate up to limit many fields",
        type=int,
        default=None,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-z",
        "--zarr",
        help="Path where zarr store will be read from.",
        type=Path,
    )
    group.add_argument("--view", help="Z3FDB View file to load", type=Path)
    parser.add_argument(
        "--fdb-config",
        help="Path to fdb config.yaml. If not set FDB_HOME will be used, only used with --view otherwise ignored",
        type=Path,
    )


@profile("aggregate-zarr")
def execute_cmd(args):
    print("Aggregating zarr array")
    if args.zarr:
        data = zarr.open_array(
            args.zarr, mode="r", zarr_format=3, use_consolidated=False
        )
    elif args.view:
        data = zarr.open_array(
            make_store(args.view, args.fdb_config),
            mode="r",
            zarr_format=3,
            use_consolidated=False,
        )
    else:
        raise Exception("Internal Error")
    val = aggregate(data, args.limit)
    print(val)
    print("Finished")

@profile("aggregate-zarr-inner-loop")
def aggregate(data, limit):
    val = 0
    indices = list(product(*(range(dim) for dim in data.shape[:-1])))
    random.shuffle(indices)
    for idx in islice(indices, limit):
        val += numpy.sum(data[idx])
    return val
