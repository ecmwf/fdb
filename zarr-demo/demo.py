#! /usr/bin/env python
# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
import pyfdb

import argparse
import logging
import math
import os
import random
import shutil
import sys
import time
import asyncio

from collections import namedtuple
from dataclasses import KW_ONLY, dataclass
from pathlib import Path

import eccodes
import numpy as np
import tqdm
import yaml
import zarr

from pychunked_data_view.chunked_data_view import (
    AxisDefinition,
    ChunkedDataViewBuilder,
    ExtractorType,
)
from z3fdb.mapping import (
    FdbSource,
    FdbZarrArray,
    FdbZarrGroup,
    FdbZarrStore,
)


def initialize_logger():
    global logger
    logger = logging.getLogger(__name__)
    logger.info("Begin")


def be_quiet_stdout_and_stderr():
    """
    Redirects current stdout/stderr to /dev/null and then recreates stdout and
    stderr. This lets libfdb and libgribjump continue to use the redirected
    FDs, i.e. send their output to /dev/null while the rest of the application
    can use stdout stderr normally.
    """
    redirect_file = open("/dev/null")

    stdout_fd = sys.stdout.fileno()
    orig_stdout_fd = os.dup(stdout_fd)
    sys.stdout.close()
    os.dup2(redirect_file.fileno(), stdout_fd)
    sys.stdout = os.fdopen(orig_stdout_fd, "w")

    stderr_fd = sys.stderr.fileno()
    orig_stderr_fd = os.dup(stderr_fd)
    sys.stderr.close()
    os.dup2(redirect_file.fileno(), stderr_fd)
    sys.stderr = os.fdopen(orig_stderr_fd, "w")


def print_in_closest_unit(val_in_ns) -> str:
    """
    Prints a nano second time value as human readable string.
    10 -> '10ns'
    1000 -> '10μs'
    1000000 - > '10ms'

    raises ValueError on values >= 1e12
    """
    units = ["ns", "μs", "ms", "s"]
    exp = int(math.log10(val_in_ns)) // 3
    if exp < len(units):
        return f"{val_in_ns / 10 ** (exp * 3)}{units[exp]}"
    raise ValueError


def create_database(path):
    db_store_path = path / "db_store"
    logger.info(f"creating db configuration at {db_store_path}")
    if db_store_path.exists():
        shutil.rmtree(db_store_path)
    db_store_path.mkdir(parents=True, exist_ok=True)

    schema_path = path / "schema"
    schema = """
        param:      Param;
        step:       Step;
        date:       Date;
        hdate:      Date;
        refdate:    Date;
        latitude:   Double;
        longitude:  Double;
        levelist:   Double;
        grid:       Grid;
        expver:     Expver;
        time:       Time;
        fcmonth:    Integer;
        number:     Integer;
        frequency:  Integer;
        direction:  Integer;
        channel:    Integer;
        instrument: Integer;
        ident:      Integer;
        diagnostic: Integer;
        iteration:  Integer;
        system:     Integer;
        method:     Integer;
        [ class, expver, stream=oper/dcda/scda, date, time, domain?

               [ type=im/sim
                       [ step?, ident, instrument, channel ]]

               [ type=ssd
                       [ step, param, ident, instrument, channel ]]

               [ type=4i, levtype
                       [ step, iteration, levelist, param ]]

               [ type=me, levtype
                       [ step, number, levelist?, param ]]

               [ type=ef, levtype
                       [ step, levelist?, param, channel? ]]

               [ type=ofb/mfb
                       [ obsgroup, reportype ]]

               [ type, levtype
                       [ step, levelist?, param ]]

        ]
        # enfo
        [ class, expver, stream=enfo/efov/eefo, date, time, domain

               [ type, levtype=dp, product?, section?
                      [ step, number?, levelist?, latitude?, longitude?, range?, param ]]

               [ type=tu, levtype, reference
                       [ step, number, levelist?, param ]]

               [ type, levtype
                       [ step, quantile?, number?, levelist?, param ]]

        ]
    """
    schema_path.write_text(schema)

    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "handler": "Default",
                "roots": [
                    {"path": str(db_store_path)},
                ],
            }
        ],
    }
    fdb_config_path = path / "fdb_config.yaml"
    fdb_config_path.write_text(yaml.dump(fdb_config))
    return fdb_config_path


def open_database(config_path: Path):
    logger.info(f"Opening fdb with config {config_path}")
    os.environ["FDB5_CONFIG_FILE"] = str(config_path)
    return pyfdb.FDB()


def open_view(fdb_config_path: Path):
    builder = ChunkedDataViewBuilder(fdb_config_path)
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2017-01-01/to/2020-12-31,"
        "levtype=sfc,"
        "step=0,"
        "param=10u/10v/2d/2t/lsm/msl/sdor/skt/slor/sp/tcw/z,"
        "time=0/to/21/by/6",
        [AxisDefinition(["date", "time"], True), AxisDefinition(["param"], True)],
        ExtractorType.GRIB,
    )
    builder.add_part(
        "type=an,"
        "class=ea,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2017-01-01/to/2020-12-31,"
        "levtype=ml,"
        "step=0,"
        "param=q/t/u/v/w/vo/d,"
        "levelist=48/60/68/74/79/83/90/96/101/105/114/120/133,"
        "time=0/to/21/by/6",
        [AxisDefinition(["date", "time"], True), AxisDefinition(["param","levelist"], True)],
        ExtractorType.GRIB,
    )
    builder.extendOnAxis(1)
    view = builder.build()
    mapping = FdbZarrStore(
        FdbZarrGroup(
            children=[
                FdbZarrArray(
                    name="data",
                    datasource=FdbSource(view),
                )
            ]
        )
    )
    return zarr.open_group(mapping, mode="r", zarr_format=3, use_consolidated=False)


def import_grib_file(fdb, grib_file, show_progress, flush_every_nth_message=256):
    logger.info(f"Archiving {grib_file}")
    grib_file = eccodes.FileReader(grib_file)
    for idx, msg in enumerate(tqdm.tqdm(grib_file, disable=not show_progress)):
        fdb.archive(msg.get_buffer())
        if (idx + 1) % flush_every_nth_message == 0:
            fdb.flush()
    fdb.flush()


def compute_mean_per_field(store) -> None:
    logger.info("Computing means")
    means_per_sample = []

    for sample in tqdm.tqdm(store["data"]):
        means_per_variable = np.mean(sample)
        means_per_sample.append(means_per_variable)
    means = np.mean(means_per_sample, axis=0)
    print(f"means={means}")


def create_db_cmd(args):
    logger.info("Creating demo database")
    create_database(args.path)


def import_data_cmd(args):
    logger.info("Importing data into demo database")
    fdb_config_path = args.database / "fdb_config.yaml"
    fdb = open_database(fdb_config_path)
    for p in args.path:
        p = p.expanduser().resolve()
        if p.is_dir():
            for f in p.iterdir():
                import_grib_file(fdb, f, args.progress)

        elif p.is_file():
            import_grib_file(fdb, p, args.progress)


def time_access(store, rand_indexes):
    timings = []
    means = []
    t1_all = time.perf_counter_ns()
    for idx in rand_indexes:
        t1 = time.perf_counter_ns()
        field = store["data"][*idx]
        means.append(np.mean(field))
        t2 = time.perf_counter_ns()
        timings.append(t2 - t1)
    t2_all = time.perf_counter_ns()
    return t2_all - t1_all, [t for t in zip(timings, means)]

def gen(shape, limit):
    for _ in range(0, limit):
        gens = [range(0, len) for len in shape]
        yield [random.choice(x) for x in gens]

def random_reads_cmd(args):
    if args.database:
        logger.info(f"Running {args.count} random reeads on fdb")
        store = open_view(args.database / "fdb_config.yaml")
    elif args.zarr_path:
        logger.info(f"Running {args.count} random reeads on zarr")
        store = zarr.open_group(
            args.zarr_path, mode="r", zarr_format=3, use_consolidated=False
        )
    else:
        logger.error("Internal error")
        sys.exit(-1)
    time_access(store, gen(store["data"].shape[:-1], args.count))
    logger.info("Completed")

def profile_cmd(args):
    fdb_store = open_view(args.database / "fdb_config.yaml")
    if not args.zarr_path.exists():
        copy_store_sync(fdb_store.store, args.zarr_path)
    zarr_store = zarr.open_group(
        args.zarr_path, mode="r", zarr_format=3, use_consolidated=False
    )

    # assert np.array_equal(fdb_store["data"], zarr_store["data"])

    shape = fdb_store["data"].shape[:-1]


    indexes = list(gen(shape, args.count))

    time_access(zarr_store, indexes)
    zarr_timings = time_access(zarr_store, indexes)
    time_access(fdb_store, indexes)
    fdb_timings = time_access(fdb_store, indexes)
    print(f"TOTOAL READ TIME ZFDB: {print_in_closest_unit(fdb_timings[0])} for {len(fdb_timings[1])} fields {args.count} reads")
    print(f"TOTOAL READ TIME ZARR-NATIVE: {print_in_closest_unit(zarr_timings[0])} for {len(zarr_timings[1])} fields {args.count} reads")
    for f, z in zip(fdb_timings[1], zarr_timings[1]):
        if f[1] != z[1]:
            print(f"{f[1]} != {z[1]}")


async def copy_store_v3_2(source_store: zarr.abc.store.Store, dest_path: str):
    copied_count = 0
    bytes_copied = 0
    dest_store = zarr.storage.LocalStore(dest_path)
    try:
        compressors = zarr.codecs.BloscCodec(
            cname="lz4", clevel=3, shuffle=zarr.codecs.BloscShuffle.bitshuffle
        )
        grp = zarr.open(source_store, mode="r", zarr_version=3)

        data = grp["data"]
        arr = zarr.create_array(
            name="data",
            store=dest_store,
            shape=data.shape,
            dtype=data.dtype,
            chunks=data.chunks,
            #compressors=compressors,
        )
        arr[:] = data[:]
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


def copy_store_sync(
    source_store: zarr.abc.store.Store,
    dest_path: str,
):
    logger.info("Copy data into native zarr store")
    return asyncio.run(copy_store_v3(source_store, dest_path))
    # return asyncio.run(copy_store_v3(source_store, dest_path))


def dump_zarr_cmd(args):
    store = open_view(args.database / "fdb_config.yaml")
    copy_store_sync(store.store, args.out)


def parse_cli_args():
    #########################################################################
    ### CMD ARGS
    #########################################################################
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-v", "--verbose", help="Enables verbose output", action="store_true"
    )
    parser.add_argument(
        "-p", "--progress", help="Show progress output", action="store_true"
    )
    sub_parsers = parser.add_subparsers(dest="cmd", required=True)

    #########################################################################
    ### CREATE-DB CMD ARGS
    #########################################################################
    create_db_parser = sub_parsers.add_parser(
        "create-db",
        help="Creates a new fdb and gribjump configuration.",
    )
    create_db_parser.set_defaults(func=create_db_cmd)
    create_db_parser.add_argument(
        "path",
        help="Path where the database will be created",
        type=Path,
        nargs="?",
        default=Path.cwd(),
    )

    #########################################################################
    ### IMPORT-DATA CMD ARGS
    #########################################################################
    import_data_parser = sub_parsers.add_parser(
        "import-data",
        help="Import data into FDB, can point to a single grib file or a directory",
    )
    import_data_parser.set_defaults(func=import_data_cmd)
    import_data_parser.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the database folder that contains configs, db_store and schema",
        default=Path.cwd(),
    )
    import_data_parser.add_argument(
        "path",
        help="Path from where to read grib file or files",
        type=Path,
        nargs="+",
    )

    #########################################################################
    ### PROFILE CMD ARGS
    #########################################################################
    profile_parser = sub_parsers.add_parser(
        "profile",
        help="Run example computation on zarr or fdb zarr. "
        "Requires an existing prepopulated database. "
        "Create the database with 'create-db'.",
    )
    profile_parser.set_defaults(func=profile_cmd)
    profile_parser.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the database folder that contains configs, db_store and schema",
        default=Path.cwd(),
    )
    profile_parser.add_argument(
        "-z",
        "--zarr-path",
        type=Path,
        help="Path where to dump zarr store to.",
        default=Path.cwd() / "dump.zarr",
    )
    profile_parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=10,
        help="Count of random reads to perform"
    )
    
    #########################################################################
    ### RANDOM READS CMD ARGS
    #########################################################################
    read_parser = sub_parsers.add_parser(
        "random-reads",
        help="Run example computation on zarr or fdb zarr. "
        "Requires an existing prepopulated database or matching zarr store"
    )
    read_parser.set_defaults(func=random_reads_cmd)
    read_parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=10,
        help="Count of random reads to perform"
    )
    group = read_parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the fdb",
    )
    group.add_argument(
        "-z",
        "--zarr-path",
        type=Path,
        help="Path to zarr store",
    )

    #########################################################################
    ### DUMP ZARR CMD ARGS
    #########################################################################
    dump_zarr_parser = sub_parsers.add_parser(
        "dump-zarr", help="Copy zfdb view into zarr store"
    )
    dump_zarr_parser.set_defaults(func=dump_zarr_cmd)
    dump_zarr_parser.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the database folder that contains configs, db_store and schema",
        default=Path.cwd(),
    )
    dump_zarr_parser.add_argument(
        "-o",
        "--out",
        help="Output path",
        type=Path,
        nargs="?",
        default=Path("dump.zarr"),
    )
    return parser.parse_args()


def main():
    """
    Use:
        create-db: Creates empty fdb
        import-data: Imports all grib file from <dir> into a fdb identified by <dir>
            Expects to be pointed to pointed to a db created with 'create-db'
        dump-zarr: Dumps zarr store
        setup-profiling: call create-db, import-data, dump-zarr
        profile: run simulated training against fdb, run simulated training against zarr

    """
    args = parse_cli_args()
    if not args.verbose:
        be_quiet_stdout_and_stderr()
    logging.basicConfig(
        format="%(asctime)s %(message)s", stream=sys.stdout, level=logging.INFO
    )
    initialize_logger()
    args.func(args)


if __name__ == "__main__":
    main()
