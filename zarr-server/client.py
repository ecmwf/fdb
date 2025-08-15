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
import sys
import argparse

import numpy
import zarr

import requests
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
    url = "http://localhost:5000/create"
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, data=json.dumps(view))

    logging.debug(response.content)
    hash = response.json()["hash"]
    z_grp = FsspecStore.from_url(
        f"http://localhost:5000/get/zarr/{hash}",
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

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--verbose",
        help="Enables verbose output, use -vv or -vvv for even more verbose output",
        action="count",
        default=2,
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
