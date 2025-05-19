# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import copy
from abc import ABC, abstractmethod
from collections.abc import Sequence
from enum import Enum, auto

import numpy as np

from z3fdb.error import ZfdbError


def is_sequence(t) -> bool:
    return not isinstance(t, str) and (
        isinstance(t, Sequence) or isinstance(t, np.ndarray)
    )


class ChunkAxis(ABC):
    @abstractmethod
    def __getitem__(self, index: int) -> dict: ...

    def __len__(self) -> int: ...

    def keys(self) -> list[str]: ...


class ChunkAxisType(Enum):
    DateTime = auto()
    Step = auto()


class ChunkDateTime(ChunkAxis):
    def __init__(self, date, time):
        self._date = date
        self._time = time

    def __getitem__(self, index: int) -> dict:
        date_idx = index // len(self._time)
        time_idx = index % len(self._time)
        return {"date": self._date[date_idx], "time": self._time[time_idx]}

    def __len__(self) -> int:
        return len(self._time) * len(self._date)

    def keys(self) -> list[str]:
        return ["date", "time"]


class ChunkSteps(ChunkAxis):
    def __init__(self, step):
        self._step = step

    def __getitem__(self, index: int) -> dict:
        return {"step": self._step[index]}

    def __len__(self) -> int:
        return len(self._step)

    def keys(self) -> list[str]:
        return ["step"]


def into_mars_request_dict(mars_request: dict) -> dict[str, str]:
    mars_request_result = copy.deepcopy(mars_request)

    if isinstance(mars_request_result, dict):
        for k, v in mars_request_result.items():
            mars_request_result[k] = into_mars_representation(v)
    else:
        raise RuntimeError("mars_request must be a dictionary.")
    return mars_request_result


def into_mars_representation(value) -> str:
    if is_sequence(value) and len(value) == 1:
        value = value[0]
    if isinstance(value, str):
        return value
    if isinstance(value, np.datetime64) and value.dtype == "datetime64[D]":
        return str(value).replace("-", "")
    if is_sequence(value):
        return "/".join([into_mars_representation(v) for v in value])
    else:
        return str(value)


class Request:
    def __init__(self, *, request, chunk_axis: ChunkAxisType):
        self._request = request
        self._template = request.copy()
        if chunk_axis == ChunkAxisType.DateTime:
            date = request["date"]
            if not is_sequence(date):
                date = [date]
            # date = into_mars_representation(date)
            if isinstance(date, np.ndarray):
                date = list(date)
            time = request["time"]
            if not is_sequence(time):
                time = [time]
            self._chunk_axis = ChunkDateTime(date, time)
        elif chunk_axis == ChunkAxisType.Step:
            step = request["step"]
            if not is_sequence(step):
                step = [step]
            self._chunk_axis = ChunkSteps(step)
        else:
            raise ZfdbError("Unknown chunking specified")
        for key in self._chunk_axis.keys():
            self._template.pop(key, None)
        self._template = into_mars_request_dict(self._template)

    def __getitem__(self, idx) -> dict:
        return self._template | into_mars_request_dict(self._chunk_axis[idx])

    def chunk_axis(self) -> ChunkAxis:
        return self._chunk_axis

