# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from typing import Any

import pytest
from zarr.abc.store import OffsetByteRequest, RangeByteRequest, SuffixByteRequest
from zarr.core.buffer import Buffer, default_buffer_prototype
from zarr.core.buffer.cpu import Buffer as CpuBuffer
from zarr.core.sync import _collect_aiterator, sync
from zarr.testing.store import StoreTests

from z3fdb._internal.zarr import FdbZarrStore
from z3fdb.z3fdb_error import Z3fdbError
from z3fdb import SimpleStoreBuilder, AxisDefinition, ExtractorType, Chunking

log = logging.getLogger(__name__)

pytestmark = pytest.mark.offline

_MARS_REQUEST = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "date": "2020-01-01/to/2020-01-04",
    "levtype": "sfc",
    "step": 0,
    "param": [167, 131, 132],
    "time": "0/to/21/by/3",
}

_AXES = [
    AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
    AxisDefinition(["param"], Chunking.SINGLE_VALUE),
]


class TestFdbZarrStore(StoreTests[FdbZarrStore, Buffer]):
    store_cls = FdbZarrStore
    buffer_cls = CpuBuffer

    # Fixtures
    @pytest.fixture
    def store_kwargs(self, read_only_fdb_setup):
        pytest.xfail("FdbZarrStore is built via SimpleStoreBuilder, not open() kwargs")

    @pytest.fixture
    def store(self, read_only_fdb_setup) -> FdbZarrStore:
        builder = SimpleStoreBuilder(read_only_fdb_setup)
        builder.add_part(_MARS_REQUEST, _AXES, ExtractorType.GRIB)
        store = builder.build()
        log.debug("store fixture: %s, known_paths=%d", type(store).__name__, len(store._known_paths))
        return store

    # Required abstract helpers
    async def get(self, store: FdbZarrStore, key: str) -> Buffer:
        """Bypass store.get() and read directly from the internal tree."""
        result = store._child[key]
        log.debug("get(key=%r) -> %s", key, type(result).__name__ if result is not None else None)
        if result is None:
            raise KeyError(key)
        return result

    async def set(self, store: FdbZarrStore, key: str, value: Buffer) -> None:
        """FdbZarrStore is read-only; skip any test that needs to inject data."""
        pytest.xfail("FdbZarrStore is read-only — cannot inject arbitrary test data via set()")

    # Required abstract tests
    def test_store_repr(self, store: FdbZarrStore) -> None:
        r = repr(store)
        log.debug("repr(store)=%r", r)
        assert "FdbZarrStore" in r

    def test_store_supports_writes(self, store: FdbZarrStore) -> None:
        log.debug("supports_writes=%r", store.supports_writes)
        assert not store.supports_writes

    def test_store_supports_listing(self, store: FdbZarrStore) -> None:
        log.debug("supports_listing=%r", store.supports_listing)
        assert store.supports_listing

    # Overrides: harness assumes a mutable store; correct for read-only
    def test_store_read_only(self, store: FdbZarrStore) -> None:
        log.debug("read_only=%r", store.read_only)
        assert store.read_only
        with pytest.raises(AttributeError):
            store.read_only = False

    def test_store_eq(self, store: FdbZarrStore, store_kwargs: dict[str, Any] = None) -> None:
        log.debug("store == store -> %r", store == store)
        assert store == store

    @pytest.mark.xfail(raises=TypeError, reason="FdbZarrStore wraps C++ objects that are not picklable")
    async def test_serializable_store(self, store: FdbZarrStore) -> None:
        import pickle

        log.debug("attempting pickle.dumps on %s", type(store).__name__)
        pickle.dumps(store)

    @pytest.mark.parametrize("read_only", [True, False])
    @pytest.mark.xfail(
        raises=TypeError,
        reason="FdbZarrStore does not support open() kwargs construction",
    )
    async def test_store_open_read_only(self, open_kwargs: dict[str, Any], read_only: bool) -> None:
        log.debug("attempting FdbZarrStore.open(read_only=%r)", read_only)
        await FdbZarrStore.open(read_only=read_only)

    @pytest.mark.xfail(
        raises=TypeError,
        reason="FdbZarrStore does not support open() kwargs construction",
    )
    async def test_store_context_manager(self, open_kwargs: dict[str, Any]) -> None:
        log.debug("attempting FdbZarrStore.open()")
        await FdbZarrStore.open()

    async def test_read_only_store_raises(self, store: FdbZarrStore) -> None:
        log.debug("read_only=%r; expecting Z3fdbError on set and delete", store.read_only)
        assert store.read_only
        with pytest.raises(Z3fdbError):
            await store.set("foo", self.buffer_cls.from_bytes(b"bar"))
        with pytest.raises(Z3fdbError):
            await store.delete("foo")

    def test_with_read_only_store(self, store: FdbZarrStore) -> None:
        log.debug("read_only=%r; expecting NotImplementedError on with_read_only", store.read_only)
        assert store.read_only
        with pytest.raises(NotImplementedError):
            store.with_read_only(read_only=False)

    # ------------------------------------------------------------------
    # Read: reimplemented against actual FDB-backed zarr structure
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        ("data", "byte_range"),
        [
            (b"\x01\x02\x03\x04", None),
            (b"\x01\x02\x03\x04", RangeByteRequest(1, 4)),
            (b"\x01\x02\x03\x04", OffsetByteRequest(1)),
            (b"\x01\x02\x03\x04", SuffixByteRequest(1)),
            (b"", None),
        ],
    )
    async def test_get(
        self,
        deep_store: FdbZarrStore,
        data: bytes,
        byte_range: RangeByteRequest | OffsetByteRequest | SuffixByteRequest | None,
    ) -> None:
        key = "zarr.json"
        log.debug("test_get(key=%r, byte_range=%r)", key, byte_range)
        if byte_range is not None:
            with pytest.raises(Z3fdbError):
                await deep_store.get(key, prototype=default_buffer_prototype(), byte_range=byte_range)
            return
        result = await deep_store.get(key, prototype=default_buffer_prototype())
        log.debug("get(%r) -> %s, len=%d", key, type(result).__name__, len(result) if result else 0)
        assert result is not None
        meta = json.loads(result.to_bytes())
        log.debug("zarr.json metadata keys: %s", list(meta.keys()))
        assert meta["zarr_format"] == 3

    @pytest.mark.xfail(
        raises=Z3fdbError,
        reason="FdbZarrStore raises Z3fdbError for any byte_range, not ValueError",
    )
    async def test_get_raises(self, store: FdbZarrStore) -> None:
        log.debug("attempting get with RangeByteRequest(0, 2), expecting Z3fdbError")
        await store.get(
            "zarr.json",
            prototype=default_buffer_prototype(),
            byte_range=RangeByteRequest(0, 2),
        )

    async def test_get_many(self, store: FdbZarrStore) -> None:
        all_keys = await _collect_aiterator(store.list())
        keys = tuple(all_keys[:5])
        log.debug("test_get_many: fetching keys=%s", keys)
        observed_buffers = await _collect_aiterator(
            store._get_many(
                zip(
                    keys,
                    (default_buffer_prototype(),) * len(keys),
                    (None,) * len(keys),
                    strict=False,
                )
            )
        )
        observed = {k: b for k, b in observed_buffers if b is not None}
        log.debug(
            "_get_many returned %d/%d buffers: %s",
            len(observed),
            len(keys),
            {k: type(v).__name__ for k, v in observed.items()},
        )
        assert set(observed.keys()) == set(keys)

    async def test_getsize(self, store: FdbZarrStore) -> None:
        result = await store.get("zarr.json", prototype=default_buffer_prototype())
        size = await store.getsize("zarr.json")
        log.debug("getsize('zarr.json')=%d, buffer len=%d", size, len(result))
        assert size == len(result)

    async def test_getsize_prefix(self, store: FdbZarrStore) -> None:
        chunk_keys = await _collect_aiterator(store.list_prefix("c/"))
        log.debug("list_prefix('c/') returned %d chunk keys", len(chunk_keys))
        expected = 0
        for k in chunk_keys:
            buf = await store.get(k, prototype=default_buffer_prototype())
            expected += len(buf)
        observed = await store.getsize_prefix("c/")
        log.debug("getsize_prefix('c/')=%d, expected=%d", observed, expected)
        assert observed == expected

    async def test_getsize_raises(self, store: FdbZarrStore) -> None:
        log.debug("getsize('nonexistent_key') should raise FileNotFoundError")
        with pytest.raises(FileNotFoundError):
            await store.getsize("nonexistent_key")

    async def test_get_bytes(self, store: FdbZarrStore) -> None:
        expected = (await store.get("zarr.json", prototype=default_buffer_prototype())).to_bytes()
        result = await store._get_bytes("zarr.json", prototype=default_buffer_prototype())
        log.debug("_get_bytes('zarr.json') -> %s, len=%d", type(result).__name__, len(result))
        assert result == expected
        log.debug("_get_bytes('nonexistent_key') should raise FileNotFoundError")
        with pytest.raises(FileNotFoundError):
            await store._get_bytes("nonexistent_key", prototype=default_buffer_prototype())

    def test_get_bytes_sync(self, store: FdbZarrStore) -> None:
        expected = sync(store.get("zarr.json", prototype=default_buffer_prototype())).to_bytes()
        result = store._get_bytes_sync("zarr.json", prototype=default_buffer_prototype())
        log.debug("_get_bytes_sync('zarr.json') -> %s, len=%d", type(result).__name__, len(result))
        assert result == expected

    async def test_get_json(self, store: FdbZarrStore) -> None:
        result = await store._get_json("zarr.json", prototype=default_buffer_prototype())
        log.debug(
            "_get_json('zarr.json') -> %s, keys=%s",
            type(result).__name__,
            list(result.keys()) if isinstance(result, dict) else result,
        )
        assert isinstance(result, dict)
        assert result["zarr_format"] == 3

    def test_get_json_sync(self, store: FdbZarrStore) -> None:
        result = store._get_json_sync("zarr.json", prototype=default_buffer_prototype())
        log.debug(
            "_get_json_sync('zarr.json') -> %s, keys=%s",
            type(result).__name__,
            list(result.keys()) if isinstance(result, dict) else result,
        )
        assert isinstance(result, dict)
        assert result["zarr_format"] == 3

    async def test_exists(self, store: FdbZarrStore) -> None:
        exists_zarr = await store.exists("zarr.json")
        exists_none = await store.exists("nonexistent/0/0/0")
        log.debug("exists('zarr.json')=%r, exists('nonexistent/0/0/0')=%r", exists_zarr, exists_none)
        assert exists_zarr
        assert not exists_none

    async def test_is_empty(self, store: FdbZarrStore) -> None:
        empty = await store.is_empty("")
        log.debug("is_empty('')=%r", empty)
        assert not empty

    # ------------------------------------------------------------------
    # Listing: reimplemented against actual FDB-backed zarr structure
    # ------------------------------------------------------------------

    async def test_list(self, store: FdbZarrStore) -> None:
        keys = await _collect_aiterator(store.list())
        log.debug("list() -> %d keys: %s", len(keys), sorted(keys))
        assert len(keys) > 0
        assert "zarr.json" in keys
        assert all(isinstance(k, str) for k in keys)

    async def test_list_prefix(self, store: FdbZarrStore) -> None:
        all_keys = await _collect_aiterator(store.list())
        log.debug("list() total keys: %d", len(all_keys))

        all_via_prefix = await _collect_aiterator(store.list_prefix(""))
        log.debug("list_prefix('') -> %d keys", len(all_via_prefix))
        assert set(all_via_prefix) == set(all_keys)

        chunk_keys = await _collect_aiterator(store.list_prefix("c/"))
        log.debug("list_prefix('c/') -> %d keys, e.g. %s", len(chunk_keys), sorted(chunk_keys)[:3])
        assert len(chunk_keys) > 0
        assert all(k.startswith("c/") for k in chunk_keys)
        assert set(chunk_keys).issubset(set(all_keys))

        zarr_keys = await _collect_aiterator(store.list_prefix("zarr"))
        log.debug("list_prefix('zarr') -> %s", zarr_keys)
        assert "zarr.json" in zarr_keys

    async def test_list_empty_path(self, store: FdbZarrStore) -> None:
        all_keys = await _collect_aiterator(store.list())
        log.debug("list() total keys: %d", len(all_keys))
        assert "zarr.json" in all_keys

        all_via_prefix = await _collect_aiterator(store.list_prefix(""))
        log.debug("list_prefix('') -> %d keys", len(all_via_prefix))
        assert set(all_via_prefix) == set(all_keys)

        sub_prefix = "c/0/"
        sub_keys = await _collect_aiterator(store.list_prefix(sub_prefix))
        log.debug("list_prefix(%r) -> %d keys, e.g. %s", sub_prefix, len(sub_keys), sorted(sub_keys)[:3])
        assert all(k.startswith(sub_prefix) for k in sub_keys)
        assert set(sub_keys).issubset(set(all_keys))

    async def test_list_dir(self, store: FdbZarrStore) -> None:
        top_level = await _collect_aiterator(store.list_dir(""))
        log.debug("list_dir('') -> %s", sorted(top_level))
        assert "zarr.json" in top_level
        assert "c" in top_level
        assert all("/" not in entry for entry in top_level)

        c_entries = await _collect_aiterator(store.list_dir("c"))
        log.debug("list_dir('c') -> %d entries: %s", len(c_entries), sorted(c_entries)[:5])
        assert len(c_entries) > 0
        assert all("/" not in entry for entry in c_entries)

    @pytest.mark.xfail(
        raises=Z3fdbError,
        reason="FdbZarrStore.get_partial_values raises Z3fdbError — not implemented",
    )
    async def test_get_partial_values(
        self, store: FdbZarrStore, key_ranges=[("non-existing", RangeByteRequest(0, 1))]
    ) -> None:
        log.debug("get_partial_values(key_ranges=%r), expecting Z3fdbError", key_ranges)
        await store.get_partial_values(
            prototype=default_buffer_prototype(),
            key_ranges=key_ranges,
        )

    # ------------------------------------------------------------------
    # Write operations: all must raise
    # ------------------------------------------------------------------

    @pytest.mark.xfail(raises=Z3fdbError, reason="FdbZarrStore is read-only — writes are not supported")
    @pytest.mark.parametrize("key", ["zarr.json", "c/0", "foo/c/0.0", "foo/0/0"])
    @pytest.mark.parametrize("data", [b"\x01\x02\x03\x04", b""])
    async def test_set(self, store: FdbZarrStore, key: str, data: bytes) -> None:
        log.debug("set(key=%r, data=%r), expecting Z3fdbError", key, data)
        await store.set(key, self.buffer_cls.from_bytes(data))

    @pytest.mark.xfail(raises=Z3fdbError, reason="FdbZarrStore is read-only — writes are not supported")
    async def test_set_many(self, store: FdbZarrStore) -> None:
        log.debug("_set_many([('zarr.json', ...)]), expecting Z3fdbError")
        await store._set_many([("zarr.json", self.buffer_cls.from_bytes(b"x"))])

    @pytest.mark.xfail(raises=Z3fdbError, reason="FdbZarrStore is read-only — writes are not supported")
    async def test_set_if_not_exists(self, store: FdbZarrStore) -> None:
        log.debug("set_if_not_exists('zarr.json', ...), expecting Z3fdbError")
        await store.set_if_not_exists("zarr.json", self.buffer_cls.from_bytes(b"x"))

    @pytest.mark.xfail(raises=NotImplementedError, reason="FdbZarrStore does not support deletes")
    async def test_clear(self, store: FdbZarrStore) -> None:
        log.debug("clear(), expecting NotImplementedError")
        await store.clear()

    @pytest.mark.xfail(raises=Z3fdbError, reason="FdbZarrStore is read-only — writes are not supported")
    async def test_delete(self, store: FdbZarrStore) -> None:
        log.debug("delete('zarr.json'), expecting Z3fdbError")
        await store.delete("zarr.json")

    @pytest.mark.xfail(raises=NotImplementedError, reason="FdbZarrStore does not support deletes")
    async def test_delete_dir(self, store: FdbZarrStore) -> None:
        log.debug("delete_dir('c'), expecting NotImplementedError")
        await store.delete_dir("c")

    @pytest.mark.xfail(raises=Z3fdbError, reason="FdbZarrStore is read-only — writes are not supported")
    async def test_delete_nonexistent_key_does_not_raise(self, store: FdbZarrStore) -> None:
        log.debug("delete('nonexistent_key'), expecting Z3fdbError")
        await store.delete("nonexistent_key")
