# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
from typing import Any, Dict, List

from pyfdb_bindings import pyfdb_bindings as pyfdb_internal

type MarsSelection = Dict[str, str]


def _flatten_values(key_values: Dict[str, Any]) -> Dict[str, str]:
    result = {}
    for k in key_values.keys():
        if isinstance(key_values[k], list):
            result[k] = "/".join(key_values[k])
        elif isinstance(key_values[k], str):
            result[k] = key_values[k]
        else:
            raise RuntimeError(
                f"MarsRequest: Value of unknown type. Expected str or list, found {type(key_values[k])}"
            )

    return result


class MarsRequest:
    def __init__(
        self, verb: str | None = None, key_values: Dict[str, Any] | None = None
    ) -> None:
        if key_values:
            key_values = _flatten_values(key_values)

        if verb is None and key_values is None:
            self.request = pyfdb_internal.MarsRequest()
        elif verb and key_values:
            self.request = pyfdb_internal.MarsRequest(verb, key_values)
        elif verb is not None:
            self.request = pyfdb_internal.MarsRequest(verb)
        elif verb or key_values:
            raise RuntimeError(
                "MarsRequest: verb and key_values can only be specified together."
            )

    @classmethod
    def _from_raw(cls, raw_mars_request: pyfdb_internal.MarsRequest):
        result = MarsRequest()
        result.request = raw_mars_request
        return result

    def verb(self) -> str:
        return self.request.verb()

    def key_values(self) -> dict[str, str]:
        return _flatten_values(self.request.key_values())

    def empty(self) -> bool:
        return self.request.empty()

    def __str__(self) -> str:
        return str(self.request)

    def __len__(self) -> int:
        return len(self.request)


class DataHandle:
    def __init__(self, dataHandle: pyfdb_internal.DataHandle):
        self.dataHandle = dataHandle

    def read(self, len: int) -> bytes:
        return self.dataHandle.read(len)


class Config:
    def __init__(self, config: str | dict = "") -> None:
        if isinstance(config, dict):
            config_str = json.dumps(config)
        elif isinstance(config, str):
            config_str = config
        else:
            raise RuntimeError("Config: Unkown config type, must be str or dict.")

        self.raw = pyfdb_internal.Config(config_str)


class FDBToolRequest:
    def __init__(
        self,
        key_values: MarsSelection | None = None,
        all: bool = False,
        minimum_key_set: List[str] = [],
    ) -> None:
        if key_values:
            key_values = _flatten_values(key_values)

        # TODO(TKR): Get rid of this retrieve dummy verb
        mars_request = MarsRequest("retrieve", key_values)

        self.tool_request = pyfdb_internal.FDBToolRequest(
            mars_request.request, all, minimum_key_set
        )

    @classmethod
    def from_mars_request(
        cls,
        mars_request: MarsRequest,
        all: bool = False,
        minimum_key_set: List[str] = [],
    ):
        if mars_request.empty():
            all = True

        return FDBToolRequest(
            key_values=mars_request.key_values(),
            all=all,
            minimum_key_set=minimum_key_set,
        )

    def mars_request(self) -> MarsRequest:
        return MarsRequest._from_raw(self.tool_request.mars_request())

    def __str__(self) -> str:
        return str(self.tool_request)


class ListElement:
    def __init__(self) -> None:
        self._element = None

    @classmethod
    def _from_raw(cls, list_element: pyfdb_internal.ListElement):
        result = ListElement()
        result._element = list_element
        return result

    def __str__(self) -> str:
        return str(self._element)


class ListIterator:
    def __init__(self) -> None:
        self._list_iterator = None

    @classmethod
    def _from_raw(cls, list_iterator: pyfdb_internal.ListIterator):
        result = ListIterator()
        result._list_iterator = list_iterator
        return result

    def __iter__(self):
        return self

    def __next__(self):
        if not self._list_iterator:
            raise StopIteration

        optional = self._list_iterator.next()

        if optional is not None:
            return ListElement._from_raw(optional)
        else:
            raise StopIteration


class WipeElement:
    def __init__(self) -> None:
        self.element = None

    @classmethod
    def _from_raw(cls, wipe_element: pyfdb_internal.WipeElement):
        result = WipeElement()
        result.element = wipe_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class WipeIterator:
    def __init__(self) -> None:
        self._iterator = None

    @classmethod
    def _from_raw(cls, iterator: pyfdb_internal.WipeIterator):
        result = WipeIterator()
        result._iterator = iterator
        return result

    def __iter__(self):
        return self

    def __next__(self):
        if not self._iterator:
            raise StopIteration

        optional = self._iterator.next()

        if optional is not None:
            return WipeElement._from_raw(optional)
        else:
            raise StopIteration


class PyFDB:
    def __init__(self, config: Config | None = None) -> None:
        pyfdb_internal.init_bindings()
        if isinstance(config, Config):
            self.FDB = pyfdb_internal.FDB(config.raw)
        else:
            self.FDB = pyfdb_internal.FDB()

    def archive(self, bytes: bytes):
        self.FDB.archive(bytes, len(bytes))

    def archive_key(self, key: str, bytes: bytes):
        self.FDB.archive(key, bytes, len(bytes))

    def archive_handle(self, mars_request: MarsRequest, data_handle: DataHandle):
        self.FDB.archive(mars_request.request, data_handle.dataHandle)

    def retrieve(self, mars_request: MarsRequest) -> DataHandle:
        return DataHandle(self.FDB.retrieve(mars_request.request))

    def list(self, fdb_tool_request: FDBToolRequest, level: int = 3):
        return ListIterator._from_raw(
            self.FDB.list(fdb_tool_request.tool_request, level)
        )

    def list_no_duplicates(self, fdb_tool_request: FDBToolRequest, level: int = 3):
        return ListIterator._from_raw(
            self.FDB.list_no_duplicates(fdb_tool_request.tool_request, level)
        )

    def wipe(
        self,
        fdb_tool_request: FDBToolRequest,
        doit: bool = False,
        porcelain: bool = False,
        unsafe_wipe_all: bool = False,
    ) -> WipeIterator:
        return WipeIterator._from_raw(
            self.FDB.wipe(
                fdb_tool_request.tool_request, doit, porcelain, unsafe_wipe_all
            )
        )

    def flush(self):
        self.FDB.flush()
