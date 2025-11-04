# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
import asyncio
import logging
import operator
from functools import reduce
from itertools import product
from pathlib import Path

import zarr.abc.store
import zarr.codecs
import zarr.storage
from tqdm import tqdm

from z3fdb_profiler.decorators import profile
from z3fdb_profiler.z3fdb import make_store

logger = logging.getLogger(__name__)


async def copy_store_async_with_compression(
    source_store: zarr.abc.store.Store, dest_path: str, show_progress: bool
):
    dest_store = zarr.storage.LocalStore(dest_path)
    try:
        compressors = zarr.codecs.BloscCodec(
            cname="lz4", clevel=3, shuffle=zarr.codecs.BloscShuffle.bitshuffle
        )
        data = zarr.open_array(source_store, mode="r", zarr_version=3)

        arr = zarr.create_array(
            store=dest_store,
            shape=data.shape,
            dtype=data.dtype,
            chunks=data.chunks,
            compressors=compressors,
        )
        for idx in tqdm(
            product(*(range(dim) for dim in data.shape[:-1])),
            disable=not show_progress,
            unit="Fields",
            total=reduce(operator.mul, data.shape[:-1], 1),
        ):
            arr[idx] = data[idx]
    finally:
        dest_store.close()

async def copy_store_v3(source_store: zarr.abc.store.Store, dest_path: str):
    copied_count = 0
    bytes_copied = 0
    dest_store = zarr.storage.LocalStore(dest_path)
    try:
        # Copy all keys from source to destination
        async for key in source_store.list():
            if copied_count % 100 == 0 and copied_count > 0:
                logger.info(f"Copied {copied_count} chunks so far. Now starting {key}")
            try:
                data = await source_store.get(key)
                if data is not None:
                    await dest_store.set(key, data)
                    bytes_copied += len(data)
                    copied_count += 1
            except Exception as e:
                print(f"ERROR COPYING FOR KEY {key}\n{e}")
                raise  # Re-raise to fail fast
        return copied_count, bytes_copied

    finally:
        dest_store.close()



def copy_store(
    source_store: zarr.abc.store.Store,
    dest_path: str,
    show_progress: bool,
):
    logger.info("Copy data into native zarr store")
    asyncio.run(
            #copy_store_async_with_compression(source_store, dest_path, show_progress)
        copy_store_v3(source_store, dest_path)
    )


def register_cmd(subparsers):
    parser = subparsers.add_parser("export", help="Export data from fdb as zarr store.")
    parser.set_defaults(func=execute_cmd)
    parser.add_argument(
        "-o",
        "--out",
        help="Path where zarr store will be written to.",
        type=Path,
        default=Path.cwd() / "zarr-export",
    )
    parser.add_argument(
        "--fdb-config",
        help="Path to fdb config.yaml. If not set FDB_HOME will be used",
        type=Path,
    )
    parser.add_argument("--view", help="View file to load", type=Path, required=True)


@profile("export")
def execute_cmd(args):
    print("Exporting view from fdb to zarr array")
    store = make_store(args.view, args.fdb_config)
    copy_store(store, args.out, args.progress)
    print("Finished")
