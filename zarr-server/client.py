#!/usr/bin/env python

# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.


import asyncio
import json
import logging
import ssl
import sys
import argparse

import numpy
import zarr

import httpx

from zarr.core.buffer import default_buffer_prototype
from zarr.storage import FsspecStore

view = {
    "requests": [
        {
            "class": "od",
            "stream": "oper",
            "expver": 1,
            "type": "fc",
            "levtype": "sfc",
            "date": "2024-01-01",
            "time": "0000",
            "step": [0, 1, 2, 3, 4],
            "param": ["165", "166"],
            "domain": "g",
        },
        {
            "class": "od",
            "stream": "oper",
            "expver": 1,
            "type": "fc",
            "levtype": "pl",
            "levelist": ["50", "100"],
            "date": "2024-01-01",
            "time": "0000",
            "step": [0, 1, 2, 3, 4],
            "param": ["133", "130"],
            "domain": "g",
        },
    ]
}


async def main():
    url = "https://localhost:5000/create"
    headers = {"Content-Type": "application/json"}

    ctx = ssl.create_default_context()
    ctx.load_cert_chain(certfile="server_data/certs/server.pem", keyfile="server_data/certs/server.pem")  # Optionally also keyfile or password.

    async with httpx.AsyncClient(http2=True, verify="server_data/certs/ca.pem") as client:
        response = await client.post(url, headers=headers, content=json.dumps(view).encode("utf-8"))

        logging.debug(response.content)
        hash = response.json()["hash"]
        z_grp = FsspecStore.from_url(
            f"https://localhost:5000/get/zarr/{hash}",
            storage_options={"client_kwargs":{"ssl": ctx}},
            #### This may fails due to the aiohttp client not being aware of the ssl certificates
            read_only=True
        )

        data_grp = zarr.open_array(z_grp, mode="r", path="/data", zarr_version=3)

        logging.debug(data_grp.shape)
        logging.debug(data_grp.chunks)

        for (x, y, z) in numpy.ndindex(data_grp.shape):
            if z > 0:
                continue
            print(f"={(x, y, z)}")
            print(data_grp[x, y, :])

        print(response.http_version)  # "HTTP/1.0", "HTTP/1.1", or "HTTP/2".

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--verbose",
        help="Enables verbose output, use -vv or -vvv for even more verbose output",
        action="count",
        default=0,
    )

    return parser.parse_args()

if __name__ == "__main__":

    args = parse_args()
    if args.verbose == 0:
        log_level = logging.WARNING
    elif args.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    logging.basicConfig(
    format="[CLIENT] - %(asctime)s %(message)s", stream=sys.stdout, level=log_level, 
    )
    # loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
