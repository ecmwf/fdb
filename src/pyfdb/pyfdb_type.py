# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from collections.abc import Collection, Mapping
from enum import IntEnum, IntFlag, auto
from pathlib import Path
from urllib import parse

from pyfdb._internal import _URI, _ControlAction, _ControlIdentifier, _DataHandle
from pyfdb._internal.pyfdb_internal import InternalMarsIdentifier, InternalMarsSelection

MarsSelection = Mapping[str, str | int | float | Collection[str | int | float]]
"""
Selection part of a MARS request.

This is a key-value map, with the data types allowed below
"""

MarsIdentifier = list[tuple[str, str]] | dict[str, str]
"""
This is the representation of a MARS identifier

This is a key-value List, mapping MARS keys to a string resembling a singluar value, see https://github.com/ecmwf/datacube-spec.
"""


class UserInputMapper:
    """
    Selection mapper for creating MARS selections.

    This class helps to create syntactically correctly structured MARS selections. If `strict_mode`
    is activated there will be checks whether keys have been set already.

    """

    @classmethod
    def map_selection_to_internal(cls, selection: MarsSelection) -> InternalMarsSelection:
        result: Mapping[str, Collection[str]] = {}

        for key, values in selection.items():
            if not isinstance(values, (int, float, str, Collection)) or isinstance(values, Mapping):
                raise ValueError(
                    f"MarsSelectionMapper: The given value for key '{key}' is not valid. A MarsSelection has to have the following type: {MarsSelection}"
                )

            # Values is a list of values but not a single string
            if isinstance(values, Collection) and not isinstance(values, str):
                converted_values = [str(v) if isinstance(v, (float, int)) else v for v in values]
                result[key] = converted_values
            # Values is a string or a float or an int
            elif isinstance(values, (int, float)):
                result[key] = [str(values)]
            elif isinstance(values, str):
                # Note: Even `to`/`to-by` range expressions are split here and treated as plain values.
                #       UserInputMapper.map_selection_to_internal() is used for all APIs (retrieve,
                #       inspect, axes, etc.). The semantic expansion of these ranges now happens in
                #       the bindings layer (see mars_requestfrom_map()), not directly in the FDB
                #       tool request. We hand a list[str] to the pybind11 layer, matching the C++
                #       splitting behaviour.
                if "/" in values:
                    result[key] = values.split("/")
                else:
                    result[key] = [values]
            else:
                raise ValueError(
                    f"Unknown type for key: {key}. Type must be int, float, str or a collection of those."
                )

        return result

    @classmethod
    def map_selection_to_external(cls, selection: InternalMarsSelection) -> MarsSelection:
        result: MarsSelection = {}

        for key, values in selection.items():
            # Values is a list of values but not a single string
            if isinstance(values, Collection) and not isinstance(values, str):
                converted_values = [str(v) if isinstance(v, (float, int)) else v for v in values]
                result[key] = converted_values
            # Values is a string or a float or an int
            elif isinstance(values, (str, int, float)):
                result[key] = str(values)
            else:
                raise ValueError(
                    f"Unknown type for key: {key}. Type must be int, float, str or a collection of those."
                )

        return result

    @classmethod
    def map_identifier_to_internal(cls, identifier: MarsIdentifier) -> InternalMarsIdentifier:
        key_values: dict[str, str] = {}

        iterator = None

        if isinstance(identifier, list):
            iterator = identifier
        elif isinstance(identifier, dict):
            iterator = identifier.items()
        else:
            raise ValueError(
                "Identifier: Unknown type for key_value_pairs. List[Tuple[str, str]] or Dict[str, str] needed"
            )

        for k, v in iterator:
            if k in key_values:
                raise KeyError(
                    f"Identifier: Key {k} already exists in Identifier: {str(key_values)}"
                )

            if isinstance(v, list) or "/" in v:
                raise ValueError(
                    "No list of values allowed. An Identifier has to be a mapping from a single key to a single value."
                )

            key_values[k] = v

        return list(key_values.items())


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
    >>>     "param": ["167", "165", "166"],
    >>>     "time": "1800",
    >>> }
    >>> data_handle = fdb.retrieve(request)
    >>> data_handle.open()
    >>> data_handle.read(4) == b"GRIB"
    >>> data_handle.close()
    >>> # OR
    >>> with fdb.retrieve(request) as data_handle:
    >>>     data_handle.read(4) == b"GRIB"
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
        >>> data_handle = fdb.retrieve(request)
        >>> data_handle.open()
        >>> data_handle.read(4) == b"GRIB"
        >>> data_handle.close()
        """
        if self.opened is True:
            return
        self.opened = True
        self.dataHandle.open()

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
        >>> data_handle = fdb.retrieve(request)
        >>> data_handle.open()
        >>> data_handle.read(4) == b"GRIB"
        >>> data_handle.close()
        """
        if self.opened is False:
            return
        self.opened = False
        self.dataHandle.close()

    def size(self) -> int:
        """
        Return the size of a data handle in bytes.

        Parameters
        ----------
        None

        Returns
        -------
        `int` describing the size of the datahandle in bytes.

        Examples
        --------
        >>> with fdb.retrieve(selection) as data_handle:
        >>>     data_handle.size() # Returns the size of the datahandle in bytes
        """
        return self.dataHandle.size()

    def read(self, len: int = -1) -> bytes:
        """
        Read a given amount of bytes from the `DataHandle`.
        This method copies the data from the underlying memory.

        Parameters
        ----------
        `len`: int
            Number of bytes to read. Defaults to -1, which reads the entire buffer.
            If the requested size exceeds the available data, the result is zero-padded to match the requested size.

        Returns
        -------
        `bytes` object resembling the read data.

        Raises
        ------
        `RuntimeError` if the DataHandle wasn't opened before the read.

        Examples
        --------
        >>> with fdb.retrieve(request) as data_handle
        >>>     data_handle.read(4) == b"GRIB"
        >>>     #or
        >>>     data_handle.read(-1) # Read the entire file
        """
        if self.opened is False:
            raise RuntimeError(
                "DataHandle: Read occured before the handle was opened. Must be opened first."
            )

        if len == -1:
            len = self.dataHandle.size()

        buffer = bytearray(len)
        read_bytes = self.dataHandle.read(buffer)

        if read_bytes == 0:
            return b""

        return bytes(buffer)

    def readinto(self, buffer: memoryview) -> int:
        """
        Read a given amount of bytes from the `DataHandle` into a `memoryview`.
        This is a zero-copy method.

        Parameters
        ----------
        `buffer`: `memoryview`
            Memory view for the buffer in which the bytes should be read

        Returns
        -------
        `int` size of bytes which have been read

        Raises
        ------
        `RuntimeError` if the DataHandle wasn't opened before the read.

        Examples
        --------
        >>> dst_read_into = io.BytesIO(b"")
        >>> with fdb.retrieve(selection) as data_handle:
        >>>     assert data_handle
        >>>     shutil.copyfileobj(data_handle, dst_read_into)
        >>>     # Reset position in file
        >>>     dst_read_into.seek(0)
        """
        if self.opened is False:
            raise RuntimeError(
                "DataHandle: Read occured before the handle was opened. Must be opened first."
            )

        # Buffer is a writable buffer (memoryview)
        return self.dataHandle.read(buffer)

    def readall(self, buffer_size: int = 1024) -> bytes:
        """
        Read all bytes from the DataHandle into memory.
        This method copies the data from the underlying memory.

        Parameters
        ----------
        `buffer_size`: `int`
            The size of the buffer which is used for reading

        Note
        ----
        *There is no need to open the DataHandle before*. This is handled by the function. The
        default chunk size is 1024 bytes.

        Returns
        -------
        `bytes` object resembling the read data

        Examples
        --------
        >>> data_handle = fdb.retrieve(request)
        >>> data_handle.readall() # Returns all content of a datahandle b"GRIB..."
        """
        buffer = bytearray()
        total_bytes_read = 0

        chunk_buf = bytearray(buffer_size)

        self.open()
        bytes_read = self.dataHandle.read(chunk_buf)

        while bytes_read > 0:
            total_bytes_read += bytes_read
            buffer.extend(chunk_buf[:bytes_read])
            bytes_read = self.dataHandle.read(chunk_buf)

        self.close()

        return bytes(buffer)

    def __repr__(self) -> str:
        return f"[{'Opened' if self.opened else 'Closed'}] Datahandle: {self.dataHandle}"


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

    def __repr__(self) -> str:
        active_identifier = "|".join([val.name for val in ControlIdentifier if self.value & val])
        if len(active_identifier) == 0:
            return "NONE"

        return active_identifier

    def __str__(self) -> str:
        return self.__repr__()


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

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.__repr__()


class URI:
    """
    Class describing a unique resource identifier.

    Parameters
    ----------
    None

    Examples
    --------
    >>> uri = URI("scheme://user:secretpass@example.com:8443/path/to/resource?query=search&sort=asc#section-2")
    """

    def __init__(self, uri: str | Path, scheme: str = "", allow_fragments=True) -> None:
        uri = uri.as_posix() if isinstance(uri, Path) else uri

        self._uri: parse.SplitResult = parse.urlsplit(
            str(uri), scheme=scheme, allow_fragments=allow_fragments
        )

    def __eq__(self, value: object, /) -> bool:
        if isinstance(value, URI):
            return parse.urlunsplit(self._uri) == parse.urlunsplit(value._uri)

        return False

    def __ne__(self, value: object, /) -> bool:
        # Needs to be implemented because of __eq__
        return not self.__eq__(value)

    def _to_internal(self):
        return _URI(parse.urlunsplit(self._uri))

    def netloc(self) -> str:
        return self._uri.netloc

    def scheme(self) -> str:
        return self._uri.scheme

    def username(self) -> str | None:
        return self._uri.username

    def password(self) -> str | None:
        return self._uri.password

    def hostname(self) -> str | None:
        return self._uri.hostname

    def port(self) -> int | None:
        return self._uri.port

    def path(self) -> str:
        return self._uri.path

    def query(self) -> str:
        return self._uri.query

    def fragment(self) -> str:
        return self._uri.fragment

    def __repr__(self) -> str:
        return parse.urlunsplit(self._uri)
