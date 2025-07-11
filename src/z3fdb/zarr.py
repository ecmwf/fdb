# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""
This module contains generator for all dot files that can
be part of a zarr store: '.zarray', '.zgroup' and '.zattrs'
"""

import dataclasses
import itertools
import json
from abc import ABC, abstractmethod
from dataclasses import KW_ONLY, asdict, dataclass, field
from functools import cache
from typing import Any, AsyncIterator, Optional, Self, Sequence, override

import numpy as np
from zarr.core.buffer import Buffer
from zarr.core.buffer.cpu import Buffer as CpuBuffer

from pychunked_data_view import ChunkedDataView

from .error import ZfdbError


def to_cpu_buffer(obj: Any) -> CpuBuffer:
    return CpuBuffer.from_bytes(json.dumps(dataclasses.asdict(obj)).encode("utf-8"))


def from_cpu_buffer(buf: CpuBuffer) -> Any:
    return json.loads(buf.to_bytes().decode("utf-8"))


@dataclass(frozen=True)
class DotZarrAttributes:
    _: KW_ONLY
    copyright: str = "ecmwf"
    zarr_format: int = 3
    variables: Sequence[dict] = field(default_factory=list)

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")

    def asstring(self) -> str:
        return json.dumps(asdict(self), indent=2)


@dataclass(frozen=True)
class MetadataConfiguration:
    _: KW_ONLY
    name: str
    configuration: dict[str, Any]


@dataclass(frozen=True)
class ChunkGridMetadata(MetadataConfiguration):
    def __init__(self, chunks) -> None:
        super().__init__(name="regular", configuration={"chunk_shape": chunks})


@dataclass
class DotZarrArrayJson:
    _: KW_ONLY
    zarr_format: int = 3
    node_type: str = "array"
    shape: tuple[int, ...]
    data_type: str | Sequence[str] | MetadataConfiguration
    chunk_grid: MetadataConfiguration
    chunk_key_encoding: MetadataConfiguration = MetadataConfiguration(
        name="default", configuration={"separator": "/"}
    )
    codecs: Sequence[MetadataConfiguration] = field(
        default_factory=lambda: [
            MetadataConfiguration(name="bytes", configuration={"endian": "little"})
        ]
    )
    fill_value: bool | int | float | None = None
    attributes: Optional[dict[str, str] | DotZarrAttributes] = field(
        default=DotZarrAttributes()
    )
    storage_transformers: Optional[Sequence[MetadataConfiguration]] = None
    dimension_names: Optional[Sequence[str | None]] = None
    # INFO: If additional fields are introduced, read the documentation about must_understand
    # https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")

    def asstring(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @property
    def __dict__(self):
        """
        get a python dictionary
        """
        return dataclasses.asdict(self)

    @property
    def json(self):
        """
        get the json formated string
        """
        return json.dumps(self.__dict__)


@dataclass()
class DotZarrGroupJson:
    _: KW_ONLY
    zarr_format: int = 3
    node_type: str = "group"
    attributes: Optional[dict[str, str] | DotZarrAttributes] = DotZarrAttributes()
    # INFO: If additional fields are introduced, read the documentation about must_understand
    # https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")

    def asstring(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @override
    def __dict__(self):
        """
        get a python dictionary
        """
        return dataclasses.asdict(self)

    @property
    def json(self):
        """
        get the json formated string
        """
        return json.dumps(self.__dict__)


class ZarrChunkedDataView:

    def __init__(self, chunked_data_view: ChunkedDataView) -> None:
        self.chunked_data_view = chunked_data_view

    @abstractmethod
    def create_dot_zarr_json(self) -> CpuBuffer: ...

    def chunks(self) -> tuple[int, ...]: 
        self.chunked_data_view.chunkShape()

    def __getitem__(self, key: tuple[int, ...]) -> Buffer: 
        self.chunked_data_view.at(key)

    @abstractmethod
    def __contains__(self, key: tuple[int, ...]) -> bool: ...


class FdbZarrArray:
    def __init__(self, *, name: str = "", datasource: ZarrChunkedDataView):
        self._name = name
        self._datasource = datasource
        self._metadata = self._datasource.create_dot_zarr_json()

    def __getitem__(self, key: tuple[str, ...]) -> Buffer | None:
        if key[0] == "zarr.json":
            return self._metadata
        # chunk_ids = [int(x) for x in key[0].split(".")]
        if len(key) > 1:
            assert key[0] == "c"  # Zarr v3 for chunks
            chunk_ids = (int(c) for c in key[1:])
            return self._datasource[*chunk_ids]

    @property
    def name(self) -> str:
        return self._name

    @cache
    def paths(self) -> list[str]:
        """
        Zarr paths associated to this array, this includes .zarray, .zattrs and all chunks.

        Returns
        -------
        list[str]
            A list of paths belonging to this group
        """
        files = ["zarr.json"]
        if len(chunks_per_axis := self._datasource.chunks()) > 0:
            tuples = itertools.product(*[np.arange(0, x) for x in chunks_per_axis])
            chunk_names = ["/".join([str(i) for i in ["c", *t]]) for t in tuples]
            files += chunk_names
        return files

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        new_prefix = prefix.removeprefix("/" + self._name)
        new_prefix_token = new_prefix.split("/")

        if len(new_prefix_token) == 1:
            yield self.name


class FdbZarrGroup:
    def __init__(
        self,
        *,
        name: str = "",
        children: list[Self | FdbZarrArray] = [],
    ):
        self._name = name
        self._metadata = to_cpu_buffer(DotZarrGroupJson())
        self._attributes = to_cpu_buffer(DotZarrAttributes())
        for c in children:
            if c.name == "":
                raise ZfdbError(
                    "A group with the empty name can only be the root group."
                )
        self._children = {c.name: c for c in children}

    def __getitem__(self, key: tuple[str, ...]) -> Buffer | None:
        if len(key) == 1:
            if key[0] == "zarr.json":
                return self._metadata
        else:
            return self._children[key[0]][*key[1:]]
        raise KeyError(f"Unknown key {key}")

    @property
    def name(self) -> str:
        return self._name

    @property
    def children(self) -> list["FdbZarrArray | FdbZarrGroup"]:
        return list(self._children.values())

    def paths(self) -> list[str]:
        """
        Zarr paths associated to this group, excluding child groups or arrays.

        Returns
        -------
        list[str]
            A list of paths belonging to this group
        """
        return ["zarr.json"]

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        new_prefix = prefix.removeprefix("/" + self._name)
        new_prefix_token = new_prefix.split("/")

        assert len(new_prefix_token) > 0

        if len(new_prefix_token) == 1:
            for k, _ in self._children.items():
                yield k
        else:
            async for i in self._children[new_prefix_token[0]].list_prefix(new_prefix):
                yield i

