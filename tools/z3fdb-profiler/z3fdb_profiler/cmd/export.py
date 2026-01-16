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
import itertools

import zarr
from zarr.codecs import BloscCodec
from tqdm.asyncio import tqdm

from z3fdb_profiler.arg_validators import accept_int_range
from z3fdb_profiler.decorators import profile
from z3fdb_profiler.z3fdb import make_store

logger = logging.getLogger(__name__)


async def copy_store_v3_compressed(
    source_store: zarr.abc.store.Store,
    dest_path: str,
    show_progress: bool,
    cname: str,
    clevel: int,
):
    dest_store = zarr.storage.LocalStore(dest_path)

    source_root = await zarr.api.asynchronous.open(store=source_store, mode="r")

    compressor = BloscCodec(cname=cname, clevel=clevel)

    bytes_copied = 0
    chunks_copied = 0

    pbar = tqdm(desc="Copying chunks", unit="chunk", disable=not show_progress)

    async def copy_array(src_arr: zarr.AsyncArray, dst_parent, name: str | None = None):
        """Copy a single array. If name is None, creates a root-level array."""
        nonlocal bytes_copied, chunks_copied

        create_kwargs = {
            "shape": src_arr.shape,
            "dtype": src_arr.dtype,
            "chunks": src_arr.chunks,
            "compressors": [compressor],
            "fill_value": src_arr.metadata.fill_value,
        }

        if src_arr.metadata.dimension_names is not None:
            create_kwargs["dimension_names"] = src_arr.metadata.dimension_names

        if name is None:
            # Root-level array
            dst_arr = await zarr.api.asynchronous.create_array(
                store=dst_parent,
                zarr_format=3,
                **create_kwargs,
            )
        else:
            # Array within a group
            dst_arr = await dst_parent.create_array(name=name, **create_kwargs)

        dst_arr.attrs.update(src_arr.attrs)

        chunk_shape = src_arr.chunks
        for chunk_coords in itertools.product(
            *(
                range((dim + chunk - 1) // chunk)
                for dim, chunk in zip(src_arr.shape, chunk_shape)
            )
        ):
            slices = tuple(
                slice(c * s, min((c + 1) * s, dim))
                for c, s, dim in zip(chunk_coords, chunk_shape, src_arr.shape)
            )

            chunk_data = await src_arr.getitem(slices)
            await dst_arr.setitem(slices, chunk_data)

            bytes_copied += chunk_data.nbytes
            chunks_copied += 1
            pbar.update(1)
            pbar.set_postfix(MB=f"{bytes_copied / 1024 / 1024:.1f}")

    async def copy_group(src_group: zarr.AsyncGroup, dst_group: zarr.AsyncGroup):
        """Recursively copy a group and all its contents."""
        dst_group.attrs.update(src_group.attrs)

        async for name, node in src_group.members():
            if isinstance(node, zarr.AsyncArray):
                await copy_array(node, dst_group, name)
            elif isinstance(node, zarr.AsyncGroup):
                new_group = await dst_group.create_group(name)
                await copy_group(node, new_group)

    try:
        if isinstance(source_root, zarr.AsyncArray):
            # Source is a standalone array
            await copy_array(source_root, dest_store, name=None)
        elif isinstance(source_root, zarr.AsyncGroup):
            # Source is a group hierarchy
            dest_group = await zarr.api.asynchronous.open_group(
                store=dest_store, mode="w", zarr_format=3
            )
            await copy_group(source_root, dest_group)
        else:
            raise TypeError(f"Unexpected source type: {type(source_root)}")

        return chunks_copied, bytes_copied
    finally:
        pbar.close()
        dest_store.close()


def copy_store(
    source_store: zarr.abc.store.Store,
    dest_path: str,
    show_progress: bool,
    cname: str,
    clevel: int,
):
    logger.info("Copy data into native zarr store")
    asyncio.run(
        copy_store_v3_compressed(source_store, dest_path, show_progress, cname, clevel)
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
    parser.add_argument(
        "--blosc-cname",
        help="Compression to use in Blosc",
        choices=["lz4", "lz4hc", "blosclz", "zstd", "snappy", "zlib"],
        default="zstd",
    )
    parser.add_argument(
        "--blosc-clevel",
        help="Compression level to use with compressor",
        type=accept_int_range(0, 10),
        default=5,
    )


@profile("export")
def execute_cmd(args):
    print("Exporting view from fdb to zarr array")
    store = make_store(args.view, args.fdb_config)
    copy_store(store, args.out, args.progress, args.blosc_cname, args.blosc_clevel)
    print("Finished")
