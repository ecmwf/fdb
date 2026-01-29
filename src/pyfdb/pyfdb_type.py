# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from enum import IntEnum, IntFlag, auto
from pathlib import Path
from typing import Collection, Dict, List, Mapping, Tuple, TypeAlias

from pyfdb._internal import (
    _URI,
    _ControlAction,
    _ControlIdentifier,
    _DataHandle,
    InternalMarsSelection,
)

"""
Selection part of a MARS request.

This is a key-value map, mapping MARS keys to a string resembling values, value lists or value ranges.
"""
MarsSelection: TypeAlias = Mapping[
    str, str | int | float | Collection[str | int | float]
]


class WildcardMarsSelection(Dict[str, str]):
    """
    Representing a wildcard MARS selection which selects all items available.
    """

    def __init__(self) -> None:
        return super().__init__()


class MarsSelectionMapper:
    """
    Selection mapper for creating MARS selections.

    This class helps to create syntactically correctly structured MARS selections. If `strict_mode`
    is activated there will be checks whether keys have been set already. If `wildcard_selection` is
    set, the resulting request will be a `WildcardMarsSelection`. A `WildcardMarsSelection` represents
    a requests which

    Examples
    --------
    TODO:
    """

    @classmethod
    def map(
        cls, selection: MarsSelection | WildcardMarsSelection
    ) -> InternalMarsSelection:
        if isinstance(selection, WildcardMarsSelection):
            return selection

        result = {}

        for key, values in selection.items():
            # Values is a list of values but not a single string
            if isinstance(values, Collection) and not isinstance(values, str):
                converted_values = [
                    str(v) if isinstance(v, float) or isinstance(v, int) else v
                    for v in values
                ]
                result[key] = "/".join(converted_values)
            # Values is a string or a float or an int
            elif (
                isinstance(values, str)
                or isinstance(values, int)
                or isinstance(values, float)
            ):
                result[key] = str(values)
            else:
                raise ValueError(
                    f"Unknown type for key: {key}. Type must be int, float, str or a collection of those."
                )

        return result


class DataHandle:
    """
    DataHandle class for lazy reading from a data source.

    Parameters
    ----------
    None

    Note
    ----
    *This class can't be instantiated / is only returned from the underlying FDB calls*

    Returns
    -------
    `DataHandle` class

    Examples
    --------
    >>> request = {
    >>>     "type": "an",
    >>>     "class": "ea",
    >>>     "domain": "g",
    >>>     "expver": "0001",
    >>>     "stream": "oper",
    >>>     "date": "20200101",
    >>>     "levtype": "sfc",
    >>>     "step": "0",
    >>>     "param": "167/165/166",
    >>>     "time": "1800",
    >>> }
    >>> data_handle = pyfdb.retrieve(request)
    >>> data_handle.open()
    >>> data_handle.read(4) == b"GRIB"
    >>> data_handle.close()
    """

    def __init__(self, dataHandle: _DataHandle, *, _internal=False):
        if not _internal:
            raise TypeError("Creating a ListElement from user code is not supported.")
        self.dataHandle: _DataHandle = dataHandle
        self.opened = False

    def __enter__(self) -> "DataHandle":
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def open(self) -> None:
        """
        Open the DataHandle object for reading.

        Parameters
        ----------
        None

        Returns
        -------
        None

        Examples
        --------
        >>> data_handle = pyfdb.retrieve(request)
        >>> data_handle.open()
        >>> data_handle.read(4) == b"GRIB"
        >>> data_handle.close()
        """
        if self.opened is True:
            return
        self.opened = True
        self.dataHandle.open_for_read()

    def close(self) -> None:
        """
        Close the DataHandle object after reading.

        Parameters
        ----------
        None

        Returns
        -------
        None

        Examples
        --------
        >>> data_handle = pyfdb.retrieve(request)
        >>> data_handle.open()
        >>> data_handle.read(4) == b"GRIB"
        >>> data_handle.close()
        """
        if self.opened is False:
            return
        self.opened = False
        self.dataHandle.close()

    def read(self, len: int) -> bytes:
        """
        Read a given amount of bytes from the DataHandle.

        Parameters
        ----------
        `len`: int
            The amount of bytes to read.

        Returns
        -------
        `bytes` object resembling the read data.

        Raises
        ------
        `RuntimeError` if the DataHandle wasn't opened before the read.

        Examples
        --------
        >>> data_handle = pyfdb.retrieve(request)
        >>> data_handle.open()
        >>> data_handle.read(4) == b"GRIB"
        >>> data_handle.close()
        """
        if self.opened is False:
            raise RuntimeError(
                "DataHandle: Read occured before the handle was opened. Must be opened first."
            )

        buffer = bytearray(len)
        self.dataHandle.read(buffer)

        return bytes(buffer)

    def readall(self) -> bytes:
        """
        Read all bytes from the DataHandle.

        Parameters
        ----------
        None

        Note
        ----
        *There is no need to open the DataHandle before*. This is handled by the function.

        Returns
        -------
        `bytes` object resembling the read data

        Examples
        --------
        >>> data_handle = pyfdb.retrieve(request)
        >>> data_handle.read_all(4) == b"GRIB"
        """
        buffer = bytearray()
        total_bytes_read = 0

        chunk_buf = bytearray(1024)

        self.open()
        bytes_read = self.dataHandle.read(chunk_buf)

        while bytes_read > 0:
            total_bytes_read += bytes_read
            buffer.extend(chunk_buf[:bytes_read])
            bytes_read = self.dataHandle.read(chunk_buf)

        self.close()

        return bytes(buffer)

    def __repr__(self) -> str:
        return (
            f"[{'Opened' if self.opened else 'Closed'}] Datahandle: {self.dataHandle}"
        )


# https://github.com/ecmwf/datacube-spec
class Identifier:
    """
    A identifier is a dictionary describing the coordinates of a datacube which point to an
    individual element or sub-datacube. Each entry in the dictionary may only have a single value.

    Parameters
    ----------
    `key_value_pairs`: `List[Tuple[str, str]]`
        A list of key-value-pairs describing the keys of a MARS request and the corresponding values

    Note
    ----
    *All inserted keys and values will be converted to lower-case. Doubled keys will be overwritten by the last occurrence.*

    Returns
    -------
    Identifier object

    Examples
    --------
    >>> identifier = Identifier([("a", "ab"), ("b", "bb"), ("c", "bc")])
    """

    def __init__(self, key_value_pairs: List[Tuple[str, str]]):
        self.key_values: Dict[str, str] = {}

        for k, v in key_value_pairs:
            if k in self.key_values.keys():
                raise KeyError(
                    f"Identifier: Key {k} already exists in Identifier: {str(self)}"
                )
            else:
                self.key_values[k] = v

    def __repr__(self) -> str:
        return ",".join([f"{k}={v}" for k, v in self.key_values.items()])


class ControlIdentifier(IntFlag):
    """
    Specify which functionality of the FDB should be addressed, e.g. RETRIEVE or LIST.

    Values
    ------
    - NONE
    - LIST
    - RETRIEVE
    - ARCHIVE
    - WIPE
    - UNIQUEROOT
    """

    NONE = 0
    LIST = auto()
    RETRIEVE = auto()
    ARCHIVE = auto()
    WIPE = auto()
    UNIQUEROOT = auto()

    @classmethod
    def _from_raw(cls, en: _ControlIdentifier):
        return ControlIdentifier[en.name]

    def _to_raw(self):
        return _ControlIdentifier[self.name]


class ControlAction(IntEnum):
    """
    Specify which action should be executed, e.g. `DISABLE` or `ENABLE`.

    Values
    ------
    - NONE
    - DISABLE
    - ENABLE
    """

    NONE = 0
    DISABLE = auto()
    ENABLE = auto()

    @classmethod
    def _from_raw(cls, en: _ControlAction):
        return ControlAction[en.name]

    def _to_raw(self):
        return _ControlAction[self.name]


class URI:
    """
    Class describing a unique resource identifier.

    Parameters
    ----------
    None

    Note
    ----
    *This class can't be default instantiated / is only returned from the underlying FDB calls*
    If you want to instantiate, use the static class methods `from_*`.

    Examples
    --------
    >>> uri = URI.from_str("scheme://netloc/path;parameters?query#fragment")
    """

    def __init__(self, uri: _URI, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a URI from user code is not supported.")
        self._uri: _URI = uri

    @classmethod
    def from_str(cls, uri: str):
        """
        Generate a `URI` from a string

        Parameters
        ----------
        `uri` : `str`
            A URI given as string

        Note
        -----
        *Currently there are only three schemes supported: file, http, https.
        Every other scheme will fall back to scheme `unix` with limited parsing support.*

        Returns
        -------
        `URI`
        """
        result = cls.__new__(cls)
        result._uri = _URI(uri)
        return result

    @classmethod
    def from_path(cls, path: Path):
        """
        Generate a `URI` from a file system path

        Parameters
        ----------
        `path` : `Path`
            A path to the file system resource

        Returns
        -------
        `URI`
        """
        result = cls.__new__(cls)
        result._uri = _URI("file", path.resolve().as_posix())
        return result

    @classmethod
    def from_scheme_uri(cls, scheme: str, uri: "URI"):
        """
        Generating a `URI` from a scheme and an existing `URI`.
        Replaces the elements of the given `URI` with the given scheme.

        Parameters
        ----------
        `scheme` : `str`
            A supported scheme
        `uri`: `URI`
            A existing URI

        Note
        -----
        *Currently there are only three schemes supported: file, http, https.
        Every other scheme will fall back to scheme `unix` with limited parsing support.*

        Returns
        -------
        `URI`
        """
        result = cls.__new__(cls)
        result._uri = _URI(scheme, uri._uri)
        return result

    @classmethod
    def from_scheme_host_port(cls, scheme: str, host: str, port: int):
        """
        Generating a `URI` from a scheme, a host and a port.

        Parameters
        ----------
        `scheme` : `str`
            A supported scheme
        `host`: `str`
            A host name
        `port`: `int`
            A port

        Note
        -----
        *Currently there are only three schemes supported: file, http, https.
        Every other scheme will fall back to scheme `unix` with limited parsing support.*

        Returns
        -------
        `URI`
        """
        result = cls.__new__(cls)
        result._uri = _URI(scheme, host, port)
        return result

    @classmethod
    def from_scheme_uri_host_port(cls, scheme: str, uri: "URI", host: str, port: int):
        """
        Generating a `URI` from a scheme, a host and a port and an existing URI.
        Replaces the elements of the given `URI` with those specified as parameters.

        Parameters
        ----------
        `scheme` : `str`
            A supported scheme
        `uri` : `URI`
            An existing URI
        `host`: `str`
            A host name
        `port`: `int`
            A port

        Note
        -----
        *Currently there are only three schemes supported: file, http, https.
        Every other scheme will fall back to scheme `unix` with limited parsing support.*

        Returns
        -------
        `URI`
        """
        result = cls.__new__(cls)
        result._uri = _URI(scheme, uri._uri, host, port)
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

    def __repr__(self) -> str:
        return repr(self._uri)
