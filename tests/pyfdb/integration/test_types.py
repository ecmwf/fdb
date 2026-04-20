# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import pytest

from pyfdb.pyfdb_iterator import (
    ControlElement,
    IndexAxis,
    ListElement,
    PurgeElement,
    StatsElement,
    StatusElement,
    WipeElement,
)
from pyfdb.pyfdb_type import DataHandle

classes = [
    IndexAxis,
    ListElement,
    StatusElement,
    PurgeElement,
    StatsElement,
    ControlElement,
    WipeElement,
    DataHandle,
]


@pytest.mark.parametrize("cls", classes)
def test_initialization_raises_error(cls):
    """Test for creation of internal data types with certain elements."""
    with pytest.raises(TypeError):
        cls(None)
