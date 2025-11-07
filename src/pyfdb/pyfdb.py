# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from pathlib import Path
from typing import Generator, List
from pyfdb import (
    URI,
    Config,
    DataHandle,
    FDBToolRequest,
    MarsRequest,
)

from pyfdb.pyfdb_iterator import (
    ControlElement,
    DumpElement,
    IndexAxis,
    ListElement,
    MoveElement,
    PurgeElement,
    StatsElement,
    StatusElement,
    WipeElement,
)

from pyfdb.pyfdb_type import Key
from pyfdb_bindings import pyfdb_bindings as pyfdb_internal


class PyFDB:
    def __init__(
        self,
        config: Config | str | dict | Path | None = None,
        user_config: Config | str | dict | Path | None = None,
    ) -> None:
        pyfdb_internal.init_bindings()

        # Convert to Config if necessary
        if config is not None and not isinstance(config, Config):
            config = Config(config)

        # Convert to Config if necessary
        if user_config is not None and not isinstance(user_config, Config):
            user_config = Config(user_config)

        if config is not None and user_config is not None:
            internal_config = pyfdb_internal.Config(config.config_str)
            internal_user_config = pyfdb_internal.Config(user_config.config_str)
            self.FDB = pyfdb_internal.FDB(internal_config, internal_user_config)
        elif config is not None:
            internal_config = pyfdb_internal.Config(config.config_str, None)
            self.FDB = pyfdb_internal.FDB(internal_config)
        else:
            self.FDB = pyfdb_internal.FDB()

    def archive(self, bytes: bytes, key: Key | None = None):
        if key is None:
            self.FDB.archive(bytes, len(bytes))
        else:
            self.FDB.archive(str(key), bytes, len(bytes))

    def flush(self):
        self.FDB.flush()

    def read(self, uri: URI) -> bytes:
        return self.FDB.read(uri._uri)

    def retrieve(self, mars_request: MarsRequest) -> DataHandle:
        return DataHandle(self.FDB.retrieve(mars_request.request))

    def list(self, fdb_tool_request: FDBToolRequest, level: int = 3):
        iterator = self.FDB.list(fdb_tool_request.tool_request, False, level)
        while True:
            try:
                yield ListElement._from_raw(next(iterator))
            except StopIteration:
                return

    def list_no_duplicates(self, fdb_tool_request: FDBToolRequest, level: int = 3):
        iterator = self.FDB.list(fdb_tool_request.tool_request, True, level)
        while True:
            try:
                yield ListElement._from_raw(next(iterator))
            except StopIteration:
                return

    def inspect(self, mars_request: MarsRequest):
        iterator = self.FDB.inspect(mars_request.request)
        while iterator is not None:
            try:
                yield DumpElement._from_raw(next(iterator))
            except StopIteration:
                return

    def dump(self, fdb_tool_request: FDBToolRequest, simple: bool = False):
        # TODO(TKR): check why this leads to a fdb5::AsyncIterationCancellation error and wipe doesn't
        iterator = self.FDB.dump(fdb_tool_request.tool_request, simple)
        while iterator is not None:
            try:
                yield DumpElement._from_raw(next(iterator))
            except StopIteration:
                return

    def status(self, fdb_tool_request: FDBToolRequest):
        iterator = self.FDB.status(fdb_tool_request.tool_request)
        while True:
            try:
                yield StatusElement._from_raw(next(iterator))
            except StopIteration:
                return

    def wipe(
        self,
        fdb_tool_request: FDBToolRequest,
        doit: bool = False,
        porcelain: bool = False,
        unsafe_wipe_all: bool = False,
    ) -> Generator[WipeElement, None, None]:
        iterator = self.FDB.wipe(
            fdb_tool_request.tool_request, doit, porcelain, unsafe_wipe_all
        )
        while True:
            try:
                yield WipeElement._from_raw(next(iterator))
            except StopIteration:
                return

    def move(self, fdb_tool_request: FDBToolRequest, destination: URI):
        iterator = self.FDB.move(fdb_tool_request.tool_request, destination._uri)
        while True:
            try:
                yield MoveElement._from_raw(next(iterator))
            except StopIteration:
                return

    def purge(
        self,
        fdb_tool_request: FDBToolRequest,
        doit: bool = False,
        porcelain: bool = False,
    ):
        iterator = self.FDB.purge(fdb_tool_request.tool_request, doit, porcelain)
        while True:
            try:
                yield PurgeElement._from_raw(next(iterator))
            except StopIteration:
                return

    def stats(self, fdb_tool_request: FDBToolRequest):
        iterator = self.FDB.stats(fdb_tool_request.tool_request)
        while True:
            try:
                yield StatsElement._from_raw(next(iterator))
            except StopIteration:
                return

    def control(
        self,
        fdb_tool_request: FDBToolRequest,
        control_action: pyfdb_internal.ControlAction,
        control_identifiers: List[pyfdb_internal.ControlIdentifier],
    ):
        iterator = self.FDB.control(
            fdb_tool_request.tool_request, control_action, control_identifiers
        )
        while True:
            try:
                yield ControlElement._from_raw(next(iterator))
            except StopIteration:
                return

    def axes(self, fdb_tool_request: FDBToolRequest, level: int = 3):
        return IndexAxis._from_raw(self.FDB.axes(fdb_tool_request.tool_request, level))

    def enabled(self, control_identifier: pyfdb_internal.ControlIdentifier) -> bool:
        return self.FDB.enabled(control_identifier)

    def needs_flush(self):
        return self.FDB.dirty()
