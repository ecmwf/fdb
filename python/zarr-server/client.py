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
import sys

import httpx
import numpy
import zarr
from dto_types import RequestDTO, RequestsDTO
from zarr.storage import FsspecStore

from pychunked_data_view import ExtractorType

request_1 = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "date": "2024-01-01/to/2024-01-31",
    "levtype": "sfc",
    "step": "0",
    "param": [
        "10u",
        "10v",
        "2d",
        "2t",
        "lsm",
        "msl",
        "sdor",
        "skt",
        "slor",
        "sp",
        "tcw",
        "z",
    ],
    "time": "0000/0600/1200/1800",
}

request_2 = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "date": "2024-01-01/to/2024-01-31",
    "levtype": "pl",
    "step": 0,
    "param": ["q", "t", "u", "v", "w"],
    "levelist": [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000],
    "time": "0000/0600/1200/1800",
}

r_dto_1 = RequestDTO(
    request_1, [(["date", "time"], True), (["param"], True)], ExtractorType.GRIB
)
r_dto_2 = RequestDTO(
    request_2,
    [(["date", "time"], True), (["param", "levelist"], True)],
    ExtractorType.GRIB,
)

r_dtos = [r_dto_1, r_dto_2]


async def main():
    url = "https://localhost:4430/create"
    headers = {"Content-Type": "application/json", "Content-Encoding": "zstd"}

    async with httpx.AsyncClient(verify=False) as client:
        response = await client.post(
            url, headers=headers, content=RequestsDTO(r_dtos).toJSON().encode("utf-8")
        )

        if response.is_client_error:
            raise RuntimeError(
                f"Encountered the following error on the server side: {response.content}"
            )

        hash = response.json()["hash"]

        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Encoding": "zstd",
        }
        z_grp = FsspecStore.from_url(
            f"https://localhost:4430/get/zarr/{hash}",
            read_only=True,
            storage_options={
                "ssl": False,
                "headers": headers,
            },  # This is verification of ssl
        )

        data_grp = zarr.open_array(z_grp, mode="r", rr_version=3)

        for x, y, z in numpy.ndindex(data_grp.shape):
            if z > 0:
                continue
            print(f"Index={(x, y, z)}")
            print(data_grp[x, y, :])

        print(data_grp.shape)
        print(data_grp.size)

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
        format="[CLIENT] - %(asctime)s %(message)s",
        stream=sys.stdout,
        level=log_level,
    )
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
