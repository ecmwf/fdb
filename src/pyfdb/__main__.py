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

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [PyFDB] %(levelname)s: %(message)s", level=logging.INFO
    )

    parser = argparse.ArgumentParser(description="pyfdb command line interface")
    parser.add_argument(
        "--print-home",
        action="store_true",
        help="Print the home directory of the fdb5 library",
    )
    parser.add_argument(
        "--findlibs-setup",
        action="store_true",
        help="Print the home directories of all pyfdb dependencies",
    )
    args = parser.parse_args()

    if not (args.print_home or args.findlibs_setup):
        parser.print_help()
        sys.exit(2)

    def _lib_home(lib_path):
        lib_dir = os.path.dirname(os.path.realpath(lib_path))
        return (
            os.path.dirname(lib_dir)
            if os.path.basename(lib_dir) in ("lib", "lib64")
            else lib_dir
        )

    if args.print_home:
        import findlibs

        lib_path = findlibs.find("fdb5")
        if lib_path is None:
            logging.error("fdb5 library not found")
            sys.exit(1)
        logging.info(f"{_lib_home(lib_path)}")

    if args.findlibs_setup:
        import findlibs

        logging.info("Findlibs Environment:")
        for key, value in os.environ.items():
            if key.upper().startswith("FINDLIBS_DISABLE"):
                logging.info(f"\t{key}: {value}")

        logging.info("Dependency Home:")
        # eckit and metkit are required; eccodes is optional
        dependencies = ["fdb5", "eckit", "metkit", "eccodes"]
        missing = []
        for lib in dependencies:
            lib_path = findlibs.find(lib)
            if lib_path is None:
                missing.append(lib)
                logging.error(f"\t{lib}: not found")
            else:
                logging.info(f"\t{lib}: {_lib_home(lib_path)}")
        if missing:
            sys.exit(1)
