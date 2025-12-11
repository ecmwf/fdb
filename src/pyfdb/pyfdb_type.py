# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
from enum import IntFlag, auto
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

import pyfdb._internal as _internal
from pyfdb._internal import (
    _URI,
    MarsRequest,
    _DataHandle,
    _flatten_values,
)

"""
Selection part of a MARS request.

This is a key-value map, mapping MARS keys to a string resembling values, value lists or value ranges.
"""
MarsSelection = Dict[str, str]


class FDBToolRequest:
    """
    FDBToolRequest object

    Parameters
    ----------
    `key_values` : `MarsSelection` | None, *optional*, default: `None`
        Dictionary of key-value pairs which describe the MARS selection
    `all` : `bool`, *optional*, default: `False`
        Should a tool request be interpreted as querying all data? True if so, False otherwise.
    `minimum_key_set` : `List[str]`, *optional*, default: `None`
        Define the minimum set of keys that must be specified. This prevents inadvertently exploring the entire FDB.

    Returns
    -------
    FDBToolRequest object

    Examples
    --------
    >>> request = FDBToolRequest(
    >>>     {
    >>>         "class": "ea",
    >>>         "domain": "g",
    >>>         "expver": "0001",
    >>>         "stream": "oper",
    >>>         "date": "20200101",
    >>>         "time": "1800",
    >>>     },
    >>>     # all = False,
    >>>     # minimum_key_set = None,
    >>> )
    """

    def __init__(
        self,
        key_values: MarsSelection | None = None,
        all: bool = False,
        minimum_key_set: List[str] | None = None,
    ) -> None:
        if key_values is not None:
            key_values = _flatten_values(key_values)

        if minimum_key_set is None:
            minimum_key_set = []

        mars_request = MarsRequest("retrieve", key_values)

        self.tool_request = _internal.FDBToolRequest(
            mars_request.request, all, minimum_key_set
        )

    def __str__(self) -> str:
        return str(self.tool_request)


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

    def __init__(self):
        self.dataHandle: _DataHandle
        self.opened = False
        raise NotImplementedError

    def __new__(cls) -> "DataHandle":
        bare_instance = object.__new__(cls)
        return bare_instance

    @classmethod
    def _from_raw(cls, data_handle: _DataHandle) -> "DataHandle":
        """
        Internal method for generating a `DataHandle` from the PyBind11 exposed element>

        Parameters
        ----------
        `data_handle` : `pyfdb._interal.DataHandle`
            Internal data handle

        Returns
        -------
        `DataHandle`
            User facing data handle
        """
        result = cls.__new__(cls)
        result.dataHandle = data_handle
        result.opened = False
        return result

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

    def read_all(self) -> bytes:
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


class Config:
    """
    Config object of the FDB. This can be used as a system or user configuration.

    Parameters
    ----------
    `config`: `str` | `dict` | `Path`
            System or user configuration

    Note
    ----
    *In case you want to supply a config to a FDB, hand in the needed arguments to the FDB directly.*
    For usage examples see the comments of PyFDB constructor.

    Every config can be a `Config` object, but is converted accordingly if another type is supplied:
        - `str` is used as a yaml representation to parse the config
        - `dict` is interpreted as hierarchical format to represent a config, see example
        - `Path` is interpreted as a location of the config and read as a YAML file

    Returns
    -------
    Config object

    Examples
    --------
    >>> config = pyfdb.Config(
            {
                "type":"local",
                "engine":"toc",
                "schema":<schema_path>,
                "spaces":[
                    {
                        "handler":"Default",
                        "roots":[
                            {"path": <db_store_path>},
                        ],
                    }
                ],
            })
    """

    def __init__(self, config: str | dict | Path) -> None:
        if isinstance(config, Path):
            self.config = yaml.safe_load(config.read_text())
        elif isinstance(config, dict):
            self.config = config
        elif isinstance(config, str):
            self.config = yaml.safe_load(config)
        else:
            raise RuntimeError(
                "Config: Unknown config type, must be str, dict or Path."
            )

    def to_json(self) -> str:
        return json.dumps(self.config)

    def __repr__(self) -> str:
        return f"Config({self.config})"


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
    *All inserted keys and values will be converted to lower-case*

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

    def __str__(self) -> str:
        return ",".join([f"{k}={v}" for k, v in self.key_values.items()])


class ControlIdentifier(IntFlag):
    """
    Specify which functionality of the FDB should be addressed, e.g. RETRIEVE or LIST.

    Values
    ----
    NONE
    LIST
    RETRIEVE
    ARCHIVE
    WIPE
    UNIQUEROOT
    """

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
    """
    Specify which action should be executed, e.g. `DISABLE` or `ENABLE`.

    Values
    ----
    NONE
    DISABLE
    ENABLE
    """

    NONE = 0
    DISABLE = auto()
    ENABLE = auto()

    @classmethod
    def _from_raw(cls, en: _internal.ControlAction):
        return ControlAction[en.name]


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

    def __init__(self):
        self._uri: _URI
        raise NotImplementedError

    def __new__(cls) -> "URI":
        bare_instance = object.__new__(cls)
        return bare_instance

    @classmethod
    def _from_raw(cls, uri: _URI) -> "URI":
        """
        Internal method for generating a `URI` from the PyBind11 exposed element

        Parameters
        ----------
        `uri` : `pyfdb._interal.URI`
            Internal URI object

        Returns
        -------
        `URI`
            User facing URI
        """
        result = cls.__new__(cls)
        result._uri = uri
        return result

    @classmethod
    def from_str(cls, uri: str):
        """
        Generate a `URI` from a string

        Parameters
        ----------
        `uri` : `str`
            A URI given as string

        Note:
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

        Note:
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

        Note:
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

        Note:
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

    def __str__(self) -> str:
        return str(self._uri)
