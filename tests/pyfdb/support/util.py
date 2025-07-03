# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from pathlib import Path

import numpy as np
from eccodes import StreamReader


def check_grib_files_for_same_content(file1: Path, file2: Path):
    with file1.open("rb") as f1:
        with file2.open("rb") as f2:
            reader1 = StreamReader(f1)
            grib1 = next(reader1)

            reader2 = StreamReader(f2)
            grib2 = next(reader2)

            return check_numpy_array_equal(grib1.data, grib2.data)


def check_numpy_array_equal(array1, array2):
    return np.array_equal(array1, array2)
