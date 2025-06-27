import json
from typing import AsyncIterator, Iterable
from zarr.abc import store

from zarr.core.buffer import Buffer, BufferPrototype, default_buffer_prototype
from zarr.core.buffer.core import Buffer as AbstractBuffer
from zarr.core.buffer.cpu import Buffer as CpuBuffer
from zarr.core.common import BytesLike

from z3fdb.zarr import FdbZarrArray, FdbZarrGroup

class FdbZarrV3Store(store.Store):

    def __init__(self, child: FdbZarrGroup | FdbZarrArray):
        super().__init__(read_only=True)

        self._child = child
        self._known_paths = self._build_paths(self._child)
        self._zmetadata = self._consolidate()

    def _build_paths(self, item, parent_path=None) -> list[str]:
        path = f"{parent_path}/{item.name}" if parent_path else item.name
        files = [f"{path}/{f}" if path != "" else f for f in item.paths()]

        if isinstance(item, FdbZarrGroup):
            for child in item.children:
                files += self._build_paths(child, path)

        return files

    def _consolidate(self):
        consolidated_metatdata = {"metadata": {}}

        ENDINGS = ["zarr.json"]

        filtered_paths = (
            path
            for path in self._known_paths
            for key in ENDINGS
            if path is not None and path.endswith(key)
        )

        for path in filtered_paths:
            keys = path.split("/")
            info = self._child[*keys]

            if info is None:
                continue

            consolidated_metatdata["metadata"].update(
                {path: json.loads(info.to_bytes())}
            )

        # Create CpuBuffer
        return CpuBuffer.from_bytes(json.dumps(consolidated_metatdata).encode("utf-8"))

    async def __getitem__(self, key) -> AbstractBuffer | None:
        if key == ".zmetadata":
            return self._zmetadata
        if key == "zarr.json":
            return self._child._metadata

        keys = key.split("/")
        return self._child[*keys]

    def __iter__(self):
        yield from iter(self._known_paths)

    def __len__(self):
        return len(self._known_paths)

    def __setitem__(self, _k, _v):
        raise NotImplementedError()

    def __delitem__(self, _k):
        raise NotImplementedError()

    def __contains__(self, key) -> bool:
        return key in self._known_paths

    def __eq__(self, value: object) -> bool:
        return self._child == value._child and self._known_paths == value._known_paths

    async def get(
        self,
        key: str,
        prototype: BufferPrototype = default_buffer_prototype(),
        byte_range: store.ByteRequest | None = None,
    ) -> Buffer | None:
        return await self.__getitem__(key)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, store.ByteRequest | None]],
    ) -> list[Buffer | None]:
        raise NotImplementedError()

    async def exists(self, key: str) -> bool:
        return key in self._known_paths

    @property
    def supports_writes(self) -> bool:
        return False

    async def set(self, key: str, value: AbstractBuffer) -> None:
        raise NotImplementedError()

    async def set_if_not_exists(self, key: str, value: AbstractBuffer) -> None:
        raise NotImplementedError()

    async def _set_many(self, values: Iterable[tuple[str, AbstractBuffer]]) -> None:
        return await super()._set_many(values)

    @property
    def supports_deletes(self) -> bool:
        return False

    async def delete(self, key: str) -> None:
        raise NotImplementedError()

    @property
    def supports_partial_writes(self) -> bool:
        return False

    async def set_partial_values(
        self, key_start_values: Iterable[tuple[str, int, BytesLike]]
    ) -> None:
        raise NotImplementedError()

    @property
    def supports_listing(self) -> bool:
        return True

    async def list(self) -> AsyncIterator[str]:
        for i in self._known_paths:
            yield i

    def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        return self._child.list_prefix(prefix)

    def list_dir(self, prefix: str) -> AsyncIterator[str]:
        return self._build_paths(self._child, parent_path=prefix)
