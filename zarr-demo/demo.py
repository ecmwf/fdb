#! /usr/bin/env python
# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
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
import pyfdb
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
        "date=2024-01-01/to/2024-01-31,"
        "levtype=sfc,"
        "step=0,"
        "param=129/134/136/151/160/163/165/166/167/168/172/235,"
        "time=0/to/21/by/6",
        [AxisDefinition(["date", "time"], True), AxisDefinition(["param"], True)],
        ExtractorType.GRIB,
    )
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


# def demo_performance_comparison(
#     fdb: pyfdb.FDB,
#     dataset: AnemoiExampleDataSet,
#     show_progress: bool,
# ) -> None:
#     if not dataset.anemoi_dataset:
#         raise Exception(
#             "Cannot compare performance without anemoi dataset as baseline."
#         )
#     print(f"Running performance comparison on {dataset.name}")
#     print("Opening fdb view")
#     fdb_view = zarr.open_group(
#         zfdb.make_anemoi_dataset_like_view(
#             recipe=yaml.safe_load(dataset.recipe.read_text()),
#             fdb=fdb,
#         ),
#         mode="r",
#         zarr_format=3,
#     )
#
#     print("Opening matching anemoi dataset")
#     anemoi = zarr.open_group(dataset.anemoi_dataset, mode="r")
#
#     def time_compute_field_sum_full_time_range(stores, idx, iterations):
#         timings = [[] for _ in stores]
#         for _ in tqdm.tqdm(range(iterations), disable=not show_progress):
#             for idx, data in enumerate(stores):
#                 t0 = time.perf_counter_ns()
#                 for chunk_idx in range(data["data"].chunks[0]):
#                     x = data["data"][chunk_idx][idx][0]
#                     np.sum(x)
#                 t1 = time.perf_counter_ns()
#                 timings[idx].append(t1 - t0)
#         return timings
#
#     def report_timings(timings):
#         mean = np.mean(timings)
#         minimum = np.min(timings)
#         maximum = np.max(timings)
#         median = np.median(timings)
#         return f"Access time per chunk:\n\tMin {print_in_closest_unit(minimum)}\n\tMean {print_in_closest_unit(mean)}\n\tMax {print_in_closest_unit(maximum)}\n\tMedian {print_in_closest_unit(median)}"
#
#     iterations = 128
#     print(
#         f"Summing 10u over {fdb_view['data'].chunks[0]} datetimes dataset / fdb multiple times (n={iterations})"
#     )
#     timings = time_compute_field_sum_full_time_range([anemoi, fdb_view], 0, iterations)
#
#     print("IO times for reading zarr from disk")
#     print(report_timings(timings[0]))
#     print("IO times for reading from fdb")
#     print(report_timings(timings[1]))
#
#
# def demo_aggeration(
#     fdb: pyfdb.FDB, gribjump: pygribjump.GribJump, dataset: AnemoiExampleDataSet
# ) -> None:
#     print(f"Running aggregation example on {dataset.name}")
#     print("Opening fdb view")
#     fdb_view = zarr.open_group(
#         zfdb.make_anemoi_dataset_like_view(
#             recipe=yaml.safe_load(dataset.recipe.read_text()),
#             fdb=fdb,
#         ),
#         mode="r",
#         zarr_format=3,
#     )
#     variable_names = fdb_view["data"].attrs["variables"]
#     means_per_sample = []
#     print(
#         f"Computing means for {len(variable_names)} variables on {fdb_view['data'].shape[0]} dates with {fdb_view['data'].shape[3]} values per field"
#     )
#     print(fdb_view["data"])
#     for sample in tqdm.tqdm(fdb_view["data"]):
#         means_per_variable = np.mean(sample, axis=2).squeeze()
#         means_per_sample.append(means_per_variable)
#     means = np.mean(means_per_sample, axis=0)
#     print("Means for each vaiable:")
#     print("\n".join([f"\t{name} = {val}" for name, val in zip(variable_names, means)]))


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


def profile_cmd(args):
    if args.source == "fdb-era5":
        dataset = [
            ds for ds in example_datasets() if isinstance(ds, AnemoiExampleDataSet)
        ][0]
        fdb = open_database(args.database / "fdb_config.yaml")
        gribjump = open_gribjump(args.database / "gribjump_config.yaml")
        store = zarr.open_group(
            zfdb.make_anemoi_dataset_like_view(
                recipe=yaml.safe_load(dataset.recipe.read_text()),
                fdb=fdb,
                gribjump=gribjump,
            ),
            mode="r",
            zarr_format=3,
        )
    elif args.source == "fdb-fc":
        dataset = [
            ds for ds in example_datasets() if isinstance(ds, ForecastExampleDataSet)
        ][0]
        fdb = open_database(args.database / "fdb_config.yaml")
        gribjump = open_gribjump(args.database / "gribjump_config.yaml")
        store = zarr.open_group(
            zfdb.make_forecast_data_view(
                request=dataset.requests,
                fdb=fdb,
                gribjump=gribjump,
            ),
            mode="r",
            zarr_format=3,
        )
    elif args.source == "zarr-era5":
        dataset = [
            ds for ds in example_datasets() if isinstance(ds, AnemoiExampleDataSet)
        ][0]
        store = zarr.open_group(dataset.anemoi_dataset, mode="r")
    else:
        logger.error(f"Unknown datasource {args.source}. Aborting.")
        sys.exit(-1)

    for _ in range(128):
        t0 = time.perf_counter_ns()
        compute_mean_per_field(store)
        t1 = time.perf_counter_ns()
        logger.info(f"Computation took {print_in_closest_unit(t1 - t0)}")


# def simulate_training_cmd(args):
#     fdb = open_database(args.database / "fdb_config.yaml")
#     gribjump = open_gribjump(args.database / "gribjump_config.yaml")
#     store = zarr.open_group(
#         zfdb.make_anemoi_dataset_like_view(
#             recipe=yaml.safe_load(args.recipe.read_text()),
#             fdb=fdb,
#             gribjump=gribjump,
#             extractor=args.extractor,
#         ),
#         mode="r",
#         zarr_format=3,
#     )
#
#     data = store["data"]
#     dates = data.shape[0]
#     base_date_access_order = list(range(0, dates - 2))
#     random.shuffle(base_date_access_order)
#
#     for idx in tqdm.tqdm(base_date_access_order, disable=not args.progress):
#         logger.info(f"Processing chunks[{idx}, {idx + 1}, {idx + 2}]")
#         np.mean(data[idx], axis=2).squeeze()
#         np.mean(data[idx + 1], axis=2).squeeze()
#         np.mean(data[idx + 2], axis=2).squeeze()
#
#
# def simulate_training_cmd2(args):
#     store = zarr.open_group(args.zstore)
#
#     data = store["data"]
#     dates = data.shape[0]
#     base_date_access_order = list(range(0, dates - 2))[: 31 * 4]
#     random.shuffle(base_date_access_order)
#
#     for idx in tqdm.tqdm(base_date_access_order, disable=not args.progress):
#         logger.info(f"Processing chunks[{idx}, {idx + 1}, {idx + 2}]")
#         np.mean(data[idx], axis=2).squeeze()
#         np.mean(data[idx + 1], axis=2).squeeze()
#         np.mean(data[idx + 2], axis=2).squeeze()


async def copy_store_v3(source_store: zarr.abc.store.Store, dest_path: str):
    copied_count = 0
    bytes_copied = 0

    print(f"source store = {source_store}")
    print(f"interface = {dir(source_store)}")

    # Create destination store - will throw if path exists or cannot be created
    dest_store = zarr.storage.LocalStore(dest_path)

    try:
        # Copy all keys from source to destination
        print(source_store)
        async for key in source_store.list():
            try:
                data = await source_store.get(key)
                print(f"{key}:{data}")
                if data is not None:
                    await dest_store.set(key, data)
                    bytes_copied += len(data)
                    copied_count += 1
            except Exception as e:
                print(e)
                raise  # Re-raise to fail fast
        return copied_count, bytes_copied

    finally:
        dest_store.close()


def copy_store_sync(
    source_store: zarr.abc.store.Store,
    dest_path: str,
):
    return asyncio.run(copy_store_v3(source_store, dest_path))


def dump_zarr_cmd(args):
    store = open_view(args.database / "fdb_config.yaml")
    compute_mean_per_field(store)
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
        "source",
        choices=["zarr-era5", "fdb-era5", "fdb-fc"],
        default="fdb-era5",
        nargs="?",
    )
    profile_parser.add_argument(
        "-d",
        "--database",
        type=Path,
        help="Path to the database folder that contains configs, db_store and schema",
        default=Path.cwd(),
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
