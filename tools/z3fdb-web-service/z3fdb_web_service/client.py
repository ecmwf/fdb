#!/usr/bin/env python

# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import argparse
import asyncio
import logging
import pathlib
import sys
import textwrap

import httpx
import numpy
import zarr
from zarr.experimental.cache_store import CacheStore
from zarr.storage import FsspecStore, MemoryStore

from z3fdb_web_service.dto_types import load_view


async def async_main(args):
    view = load_view(args.view)
    host = args.host
    url = f"{host}"
    headers = {"Content-Type": "application/json", "Content-Encoding": "zstd"}

    async with httpx.AsyncClient(verify=False) as client:
        response = await client.post(
            f"{url}/create",
            headers=headers,
            content=view.model_dump_json().encode("utf-8"),
        )

        if response.is_client_error:
            raise RuntimeError(
                "Encountered the following error on the server side: "
                f"{response.content}"
            )

        hash = response.json()["hash"]

        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Encoding": "zstd",
        }

        fs_spec_store = FsspecStore.from_url(
            f"{url}/get/zarr/{hash}",
            read_only=True,
            storage_options={
                "ssl": False,
                "headers": headers,
            },  # This is verification of ssl
        )
        if args.cache_size != 0:
            GiB = 1024**3
            cache_store = MemoryStore()
            store = CacheStore(
                store=fs_spec_store,
                cache_store=cache_store,
                max_size=args.cache_size * GiB,
            )
        else:
            store = fs_spec_store

        data_grp = zarr.open_array(store, mode="r", zarr_version=3)

        for idx in numpy.ndindex(data_grp.shape[:-1]):
            print(f"Index={idx}")
            print(data_grp[*idx, :])

        print(data_grp.shape)
        print(data_grp.size)

        print(response.http_version)  # "HTTP/1.0", "HTTP/1.1", or "HTTP/2".


def non_negative_int(value):
    n = int(value)
    if n < 0:
        raise argparse.ArgumentTypeError(f"{value} is not a non-negative integer")
    return n


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", help="Host:Port string to connect to", default="localhost:5000"
    )
    parser.add_argument(
        "view",
        help=textwrap.dedent("""
        Expects a yaml file as input describing the view to use:
        ---
        name: "Example View"
        parts:
          - request: |
              domain=g,
              class=od,
              date=-2,
              expver=1,
              levelist=50/70/100/150/200
              levtype=pl,
              param=129/130,
              step=0/1,
              stream=oper,
              time=00,
              type=fc
            axis:
              - keywords: ["step"]
                chunking: SINGLE_VALUE
              - keywords: ["param", "levelist"]
                chunking: SINGLE_VALUE
        extension_axis: ~"""),
        type=pathlib.Path,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Enables verbose output, use -vv or -vvv for even more verbose output",
        action="count",
        default=0,
    )
    parser.add_argument(
        "--cache-size",
        help="Cachesize in GB, set to 0 to disable. Must be >= 0",
        default=4,
        type=non_negative_int,
    )

    return parser.parse_args()


def main():
    args = parse_args()
    if args.verbose == 0:
        log_level = logging.WARNING
    elif args.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    logging.basicConfig(
        format="[CLIENT] - %(asctime)s %(message)s",
        stream=sys.stdout,
        level=log_level,
    )
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        pass
