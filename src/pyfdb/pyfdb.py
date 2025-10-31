# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from typing import Any, Dict
from pyfdb_bindings import pyfdb_bindings as pyfdb_internal

import json


class MarsRequest:
    def __init__(
        self, verb: str | None = None, key_values: Dict[str, Any] | None = None
    ) -> None:
        if key_values:
            key_values = MarsRequest._flatten_values(key_values)

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
    def _flatten_values(cls, key_values: Dict[str, Any]) -> Dict[str, str]:
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

    def verb(self) -> str:
        return self.request.verb()

    def key_values(self) -> dict[str, str]:
        return MarsRequest._flatten_values(self.request.key_values())


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


class PyFDB:
    def __init__(self, config: Config | None = None) -> None:
        pyfdb_internal.init_bindings()
        if isinstance(config, Config):
            self.FDB = pyfdb_internal.FDB(config.raw)
        else:
            self.FDB = pyfdb_internal.FDB()

    def archive(self, bytes: bytes):
        self.FDB.archive(bytes, len(bytes))

    def retrieve(self, mars_request: MarsRequest) -> DataHandle:
        return DataHandle(self.FDB.retrieve(mars_request.request))

    def flush(self):
        self.FDB.flush()
