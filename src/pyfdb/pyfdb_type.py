# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Tuple

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


class DataHandle:
    def __init__(self, dataHandle: pyfdb_internal.DataHandle):
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
            raise RuntimeError("Config: Unkown config type, must be str or dict.")


class Key:
    def __init__(self, key_value_pairs: List[Tuple[str, str]]):
        self.key_values = DefaultDict(list)

        for k, v in key_value_pairs:
            self.key_values[k].append(v)

    # TODO(TKR): Think whether this should only accept a single value per key
    def __str__(self) -> str:
        return ",".join([f"{k}={'/'.join(v)}" for k, v in self.key_values.items()])


class URI:
    def __init__(self):
        self._uri: pyfdb_internal.URI = pyfdb_internal.URI()

    @classmethod
    def from_str(cls, uri: str):
        result = URI()
        result._uri = pyfdb_internal.URI(uri)
        return result

    @classmethod
    def _from_raw(cls, uri: pyfdb_internal.URI):
        result = URI()
        result._uri = uri
        return result

    @classmethod
    def from_scheme_path(cls, scheme: str, path: Path):
        result = URI()
        result._uri = pyfdb_internal.URI(scheme, path.resolve().as_posix())
        return result

    @classmethod
    def from_scheme_uri(cls, scheme: str, uri: "URI"):
        result = URI()
        result._uri = pyfdb_internal.URI(scheme, uri._uri)
        return result

    @classmethod
    def from_scheme_host_port(cls, scheme: str, host: str, port: int):
        result = URI()
        result._uri = pyfdb_internal.URI(scheme, host, port)
        return result

    @classmethod
    def from_scheme_uri_host_port(cls, scheme: str, uri: "URI", host: str, port: int):
        result = URI()
        result._uri = pyfdb_internal.URI(scheme, uri._uri, host, port)
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
