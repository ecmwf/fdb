# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import argparse
import logging
import os
import sys

import findlibs

import pyfdb._internal as _internal

# INFO: This is in place because we currently can't (at runtime)
# tell which order the dependencies are in. This needs to be available in findlibs
DEPENDENCY_ORDER = ["eckit", "eccodes", "metkit", "fdb5"]
OPTIONAL_DEPENDENCIES = ["eccodes"]


def main():
    parser = argparse.ArgumentParser(description="pyfdb command line interface")
    parser.add_argument(
        "--print-home",
        action="store_true",
        help="Print the home directory of the fdb5 library",
    )
    parser.add_argument(
        "--print-home-deps",
        action="store_true",
        help="Print the home directories of all pyfdb dependencies",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s | %(levelname)-6s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if args.verbose else logging.INFO,
    )

    if not (args.print_home or args.print_home_deps):
        parser.print_help()
        sys.exit(2)

    def _lib_home(lib_path):
        lib_dir = os.path.dirname(os.path.realpath(lib_path))
        return (
            os.path.dirname(lib_dir)
            if os.path.basename(lib_dir) in ("lib", "lib64")
            else lib_dir
        )

    def _print_dep_path(lib, dependency_path, optional):
        missing = []
        if dependency_path is None:
            msg = f"\t{lib} [Optional]" if optional else f"\t{lib}"
            msg += ": not found by findlibs"
            missing.append(lib)
            if optional:
                logging.info(msg)
            else:
                logging.error(msg)
        else:
            msg = f"\t{lib} [Optional]" if optional else f"\t{lib}"
            msg += f": {_lib_home(dependency_path)}"
            logging.info(msg)

        return missing

    library_info_tuple = _internal.version_info()

    if args.print_home:
        dependency_path = findlibs.find("fdb5")
        if dependency_path is None:
            logging.error("fdb5 library not found by findlibs")
            sys.exit(1)
        for name, version, gitSha, path in library_info_tuple:
            if name == "fdb":
                logging.info(f"\t{name} {version} ({gitSha}) {path}")

    if args.print_home_deps:
        logging.info("Findlibs Environment:")
        for key, value in os.environ.items():
            if key.upper().startswith("FINDLIBS_DISABLE"):
                logging.info(f"\t{key: <15}: {value: <10}")

        logging.info("Findlibs Lookup")

        missing = []
        for lib in DEPENDENCY_ORDER:
            dependency_path = findlibs.find(lib)
            missing += _print_dep_path(
                lib, dependency_path, lib in OPTIONAL_DEPENDENCIES
            )

        logging.info("Dependency Versions:")

        for name, version, gitSha, path in library_info_tuple:
            logging.info(f"\t{name} {version} ({gitSha}) {path}")

        if missing:
            sys.exit(1)


if __name__ == "__main__":
    main()
