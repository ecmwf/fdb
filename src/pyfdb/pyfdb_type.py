# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from enum import IntFlag, auto
import json
from pathlib import Path
from typing import DefaultDict, Dict, List, Tuple

import pyfdb._internal as _internal
from pyfdb._internal import URI, MarsRequest, _flatten_values

"""
Selection part of a MARS request.

This is a key-value map, mapping MARS keys to a string resembling values, value lists or value ranges.
"""
type MarsSelection = Dict[str, str]


class FDBToolRequest:
    def __init__(
        self,
        key_values: MarsSelection | None = None,
        all: bool = False,
        minimum_key_set: List[str] | None = None,
    ) -> None:
        if key_values:
            key_values = _flatten_values(key_values)

        if minimum_key_set is None:
            minimum_key_set = []

        # TODO(TKR): Get rid of this retrieve dummy verb
        mars_request = MarsRequest("retrieve", key_values)

        self.tool_request = _internal.FDBToolRequest(
            mars_request.request, all, minimum_key_set
        )

    def __str__(self) -> str:
        return str(self.tool_request)


class DataHandle:
    def __init__(self, dataHandle: _internal.DataHandle):
        self.dataHandle = dataHandle

    def read(self, len: int) -> bytes:
        return self.dataHandle.read(len)


class Config:
    def __init__(self, config: str | dict | Path = "") -> None:
        if isinstance(config, Path):
            self.config_str = config.read_text()
        elif isinstance(config, dict):
            self.config_str = json.dumps(config)
        elif isinstance(config, str):
            self.config_str = config
        else:
            raise RuntimeError("Config: Unknown config type, must be str or dict.")


class Key:
    def __init__(self, key_value_pairs: List[Tuple[str, str]]):
        self.key_values = DefaultDict(list)

        for k, v in key_value_pairs:
            self.key_values[k].append(v)

    # TODO(TKR): Think whether this should only accept a single value per key
    def __str__(self) -> str:
        return ",".join([f"{k}={'/'.join(v)}" for k, v in self.key_values.items()])


class ControlIdentifier(IntFlag):
    NONE = 0
    LIST = auto()
    RETRIEVE = auto()
    ARCHIVE = auto()
    WIPE = auto()
    UNIQUEROOT = auto()

    @classmethod
    def _from_raw(cls, en: _internal.ControlIdentifier):
        return ControlIdentifier[en.name]


class ControlAction(IntFlag):
    NONE = 0
    DISABLE = auto()
    ENABLE = auto()

    @classmethod
    def _from_raw(cls, en: _internal.ControlAction):
        return ControlAction[en.name]


class URI:
    def __init__(self):
        self._uri: _internal.URI = _internal.URI()

    @classmethod
    def from_str(cls, uri: str):
        result = URI()
        result._uri = _internal.URI(uri)
        return result

    @classmethod
    def _from_raw(cls, uri: _internal.URI):
        result = URI()
        result._uri = uri
        return result

    @classmethod
    def from_scheme_path(cls, scheme: str, path: Path):
        result = URI()
        result._uri = _internal.URI(scheme, path.resolve().as_posix())
        return result

    @classmethod
    def from_scheme_uri(cls, scheme: str, uri: "URI"):
        result = URI()
        result._uri = _internal.URI(scheme, uri._uri)
        return result

    @classmethod
    def from_scheme_host_port(cls, scheme: str, host: str, port: int):
        result = URI()
        result._uri = _internal.URI(scheme, host, port)
        return result

    @classmethod
    def from_scheme_uri_host_port(cls, scheme: str, uri: "URI", host: str, port: int):
        result = URI()
        result._uri = _internal.URI(scheme, uri._uri, host, port)
        return result

    def name(self) -> str:
        return self._uri.name()

    def scheme(self) -> str:
        return self._uri.scheme()

    def user(self) -> str:
        return self._uri.user()

    def host(self) -> str:
        return self._uri.host()

    def port(self) -> int:
        return self._uri.port()

    def path(self) -> str:
        return self._uri.path()

    def fragment(self) -> str:
        return self._uri.fragment()

    def rawStr(self) -> str:
        return self._uri.rawString()

    def __str__(self) -> str:
        return str(self._uri)
