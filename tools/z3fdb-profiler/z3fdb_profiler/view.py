# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel


class Axis(BaseModel):
    keywords: list[str]
    chunking: bool


class Part(BaseModel):
    request: str
    axis: list[Axis]


class View(BaseModel):
    name: str
    parts: list[Part]
    extension_axis: Optional[int]


def load_view(path: Path):
    with open(path) as f:
        data = yaml.safe_load(f)
        return View(**data)
