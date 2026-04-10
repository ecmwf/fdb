#! /usr/bin/env python

# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import pathlib

import earthkit.data as ekd


def make_requests():
    common_keys = {
        "grid": "O96",
        "time": "0000/to/1800",
        "date": "2020-01-01/2020-01-02",
    }

    base_requests = [
        {
            "class": "ea",
            "levtype": "sfc",
            "param": [
                "10u",
                "10v",
            ],
        },
        {
            "class": "ea",
            "levtype": "pl",
            "param": ["u", "v"],
            "level": [50, 100],
        },
    ]
    return [req | common_keys for req in base_requests]


def main():
    for idx, req in enumerate(make_requests()):
        filename = f"{idx}_testdata.grib"
        if pathlib.Path(filename).is_file():
            print(f"File {filename} already exists, skipping download.")
            continue
        res = ekd.from_source("mars", req)
        res.save(filename)


if __name__ == "__main__":
    main()
