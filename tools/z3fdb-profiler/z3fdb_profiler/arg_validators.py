# (C) Copyright 2026- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
import argparse


def accept_int_range(lower_bound: int, upper_bound: int):
    """
    Create int range validator for argparse.

    Ensures value are in half open interval [lower_bound, upper_bound)

    Args:
        lower_bound(int): lower bound (inclusive) of the range
        upper_bound(int): upper bound (exclusive) of the range

    Returns:
        Function to be called by argparse

    Raises:
        ValueError if lower_bound >= upper_bound
    """
    if lower_bound >= upper_bound:
        raise ValueError("lower_bound must be < upper_bound")

    def validate(value: int) -> int:
        if not (lower_bound <= value < upper_bound):
            raise argparse.ArgumentTypeError(
                f"{value} must be in interval [{lower_bound}, {upper_bound})"
            )
        return value

    return validate
