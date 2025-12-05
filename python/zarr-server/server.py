#!/usr/bin/env python

# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.


import argparse
import json
import logging
import os
import pathlib
from typing import Collection

import pyfdb
import pygribjump
from dto_types import Request, RequestDTOMapper
from quart import Quart, Response, jsonify, request

# import create_certs
app = Quart(__name__)
view_hashes = {}

from z3fdb import (
    SimpleStoreBuilder,
)


def open_view(fdb_config_path: pathlib.Path, requests: Collection[Request]):
    builder = SimpleStoreBuilder(fdb_config_path)

    for r in requests:
        builder.add_part(r.request, r.axes, r.extractor)
    builder.extendOnAxis(1)
    store = builder.build()
    return store


@app.route("/create", methods=["POST"])
async def process_json():
    data = await request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400
    hashed_request = hash(json.dumps(data))

    if hashed_request not in view_hashes:
        try:
            requests = RequestDTOMapper.map_json(data)
            store = open_view(args.fdb_config, requests)
        except Exception as e:
            app.logger.info(f"Create view failed with exception: {e}")
            return jsonify({"error": f"Invalid Request - {e}"}), 400

        view_hashes[hashed_request] = store
        app.logger.debug(
            f"Created new zfdb view {hashed_request}, {len(view_hashes)} views are now opened"
        )
    else:
        app.logger.debug("Using created request")

    app.logger.debug(json.dumps({"hash": hashed_request}))

    return Response(
        response=json.dumps({"hash": hashed_request}),
        status=200,
        content_type="application-type/json",
    )


@app.route("/get/zarr/<hash>/<path:zarr_path>", methods=["GET"])
async def retrieve_zarr(hash, zarr_path):
    try:
        store = view_hashes[int(hash)]
    except KeyError:
        return Response(response=f"Couldn't find hash in {hash}", status=500)

    try:
        content = await store[zarr_path]
    except KeyError:
        return Response(
            response=f"Didn't find {zarr_path} for mapping of hash {int(hash)}",
            status=404,
        )

    if content is None:
        return Response(
            response=f"Didn't find {zarr_path} for mapping of hash {int(hash)}",
            status=404,
        )

    return Response(
        response=content.to_bytes(), content_type="application/octet-stream", status=200
    )


def log_environment():
    variables = [
        "FDB_HOME",
        "FDB5_CONFIG_FILE",
        "FDB_ENABLE_GRIBJUMP",
        "GRIBJUMP_HOME",
        "GRIBJUMP_CONFIG_FILE",
        "GRIBJUMP_IGNORE_GRID",
    ]
    for var in variables:
        try:
            val = os.environ[var]
        except KeyError:
            val = "<NOT-SET>"
        app.logger.info(f"{var}={val}")


def connect_to_fdb(args):
    if args.fdb_config:
        abs_path = args.fdb_config.expanduser().resolve()
        if not abs_path.is_file():
            raise Exception(f"Cannot find fdb config file {abs_path}")
        os.environ["FDB5_CONFIG_FILE"] = f"{abs_path}"
    os.environ["FDB_ENABLE_GRIBJUMP"] = "1"
    if args.gribjump_config:
        abs_path = args.gribjump_config.expanduser().resolve()
        if not abs_path.is_file():
            raise Exception(f"Cannot find gribjump config file {abs_path}")
        os.environ["GRIBJUMP_CONFIG_FILE"] = f"{abs_path}"
    os.environ["GRIBJUMP_IGNORE_GRID"] = "1"

    log_environment()

    global fdb
    fdb = pyfdb.FDB()
    global gribjump
    gribjump = pygribjump.GribJump()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--verbose",
        help="Enables verbose output, use -vv or -vvv for even more verbose output",
        action="count",
        default=0,
    )
    parser.add_argument("--debug", help="Enables Quart debug", action="store_true")
    parser.add_argument(
        "--fdb-config",
        help="path to fdb config file, if not specified fdb searchs as usual",
        type=pathlib.Path,
        default=None,
    )
    parser.add_argument(
        "--gribjump-config",
        help="path to gribjump config file, if not specified gribjump searchs as usual",
        type=pathlib.Path,
        default=None,
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

    connect_to_fdb(args)

    app.run(debug=args.debug, host="0.0.0.0")
