# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from pyfdb import URI, Config, DataHandle, FDBToolRequest, MarsRequest
from pyfdb.pyfdb_iterator import (
    DumpIterator,
    ListIterator,
    MoveIterator,
    PurgeIterator,
    StatusIterator,
    WipeIterator,
)
from pyfdb_bindings import pyfdb_bindings as pyfdb_internal


class PyFDB:
    # TODO(TKR): Two configs, system and user config as dicts
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

    # def reindex(self, key: Key, str], field_location: ):
    #     self.FDB.reindex(key, field_location)

    def flush(self):
        self.FDB.flush()

    def read(self, uri: URI) -> DataHandle:
        return self.FDB.read(uri._uri)

    def retrieve(self, mars_request: MarsRequest) -> DataHandle:
        return DataHandle(self.FDB.retrieve(mars_request.request))

    def list(self, fdb_tool_request: FDBToolRequest, level: int = 3):
        return ListIterator._from_raw(
            self.FDB.list(fdb_tool_request.tool_request, False, level)
        )

    def list_no_duplicates(self, fdb_tool_request: FDBToolRequest, level: int = 3):
        return ListIterator._from_raw(
            self.FDB.list_no_duplicates(fdb_tool_request.tool_request, True, level)
        )

    def inspect(self, mars_request: MarsRequest):
        return ListIterator._from_raw(self.FDB.inspect(mars_request.request))

    def dump(self, fdb_tool_request: FDBToolRequest, simple: bool = False):
        """TODO(TKR) check why this leads to a fdb5::AsyncIterationCancellation error and wipe doesn't

        Args:
            fdb_tool_request: [TODO:description]
            simple: [TODO:description]

        Returns:
            [TODO:return]
        """
        return DumpIterator._from_raw(
            self.FDB.dump(fdb_tool_request.tool_request, simple)
        )

    def status(self, fdb_tool_request: FDBToolRequest):
        return StatusIterator._from_raw(self.FDB.status(fdb_tool_request.tool_request))

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

    def move(self, fdb_tool_request: FDBToolRequest, destination: URI):
        return MoveIterator._from_raw(
            self.FDB.move(fdb_tool_request.tool_request, destination._uri)
        )

    def purge(
        self,
        fdb_tool_request: FDBToolRequest,
        doit: bool = False,
        porcelain: bool = False,
    ):
        return PurgeIterator._from_raw(
            self.FDB.purge(fdb_tool_request.tool_request, doit, porcelain)
        )
