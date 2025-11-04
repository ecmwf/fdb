# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
import argparse
import importlib
import logging
import os
import pkgutil
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


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


def register_commands(subparsers):
    """Auto-discover and register all commands from cmd/ folder."""
    cmd_path = Path(__file__).parent / "cmd"
    loaded_count = 0
    failed_count = 0

    for _, module_name, is_pkg in pkgutil.iter_modules([str(cmd_path)]):
        if is_pkg:
            continue

        try:
            full_module_name = f"z3fdb_profiler.cmd.{module_name}"
            module = importlib.import_module(full_module_name)

            if hasattr(module, "register_cmd") and callable(module.register_cmd):
                module.register_cmd(subparsers)
                loaded_count += 1
                logger.debug(f"Loaded command: {module_name}")
            else:
                logger.warning(f"Module {module_name} missing register_cmd function")
                failed_count += 1

        except ImportError as e:
            logger.error(f"Failed to import {module_name}: {e}")
            failed_count += 1
        except Exception as e:
            logger.error(f"Error registering command {module_name}: {e}", exc_info=True)
            failed_count += 1

    logger.info(f"Loaded {loaded_count} commands, {failed_count} failed")


def main():
    parser = argparse.ArgumentParser(description="Z3FDB Profiler")
    parser.add_argument(
        "-v",
        "--verbose",
        help="Enables verbose output, use -vv for more and -vvv for most verbose output",
        action="count",
        default=0,
    )
    parser.add_argument(
        "-p", "--progress", help="Show progress output", action="store_true"
    )
    subparsers = parser.add_subparsers(dest="cmd", help="Available commands")
    register_commands(subparsers)
    args = parser.parse_args()

    
    if not args.verbose < 3:
        be_quiet_stdout_and_stderr()

    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s "
    if args.verbose == 0:
        logging.basicConfig(
            format=fmt, stream=sys.stdout, level=logging.WARNING
        )
    elif args.verbose == 1:
        logging.basicConfig(
            format=fmt, stream=sys.stdout, level=logging.INFO
        )
    else:
        logging.basicConfig(
            format=fmt, stream=sys.stdout, level=logging.DEBUG
        )


    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
