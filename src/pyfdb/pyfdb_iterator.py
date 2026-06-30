# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import logging
from collections.abc import Collection, ItemsView, Iterator, KeysView, ValuesView
from enum import IntEnum, auto
from typing import Optional

from pyfdb._internal import (
    ControlElement as _ControlElement,
)
from pyfdb._internal import (
    IndexAxis as _IndexAxis,
)
from pyfdb._internal import (
    ListElement as _ListElement,
)
from pyfdb._internal import (
    PurgeElement as _PurgeElement,
)
from pyfdb._internal import (
    StatsElement as _StatsElement,
)
from pyfdb._internal import (
    WipeElement as _WipeElement,
)
from pyfdb._internal import (
    WipeElementType as _WipeElementType,
)
from pyfdb._internal.pyfdb_internal import InternalMarsSelection
from pyfdb.pyfdb_type import URI, ControlIdentifier, DataHandle, MarsSelection, UserInputMapper

logger = logging.getLogger(__name__)


class ListElement:
    """Element returned from a listing command"""

    def __init__(self, list_element: _ListElement, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a ListElement from user code is not supported.")
        self._element: _ListElement = list_element

    def has_location(self) -> bool:
        """
        Does the `ListElement` have a location

        Returns
        -------
        `bool`
            `True` if this element has a location, `False` otherwise.

        Note
        ----
        Only for `ListElement`s which are on the third level of the schema.
        """
        return self._element.has_location()

    def offset(self) -> Optional[int]:
        """
        Offset within the file associated with the `ListElement`

        Returns
        -------
        `Optional[int]`
            Offset in bytes in the data file of the FDB, if `ListElement` is on `level` 3, None otherwise.

        Note
        ----
        Only for `ListElement`s which are on the third level of the schema.
        """
        if self.has_location():
            return self._element.offset()
        logger.info("Asking for offset on list element without location. Did you specify `level=3` in a list call?")
        return None

    def length(self) -> Optional[int]:
        """
        Size of the `ListElement` within the file associated.

        Returns
        -------
        `Optional[int]`
            Size in bytes in the data file of the FDB, if `ListElement` is on `level` 3, None otherwise.

        Note
        ----
        Only for `ListElement`s which are on the third level of the schema.
        """
        if self.has_location():
            return self._element.length()
        logger.info("Asking for length on list element without location. Did you specify `level=3` in a list call?")
        return None

    def combined_key(self) -> dict[str, str]:
        """
        Return the MARS keys of the `ListElement`

        Returns
        -------
        `dict[str, str]`
            Dictionary containing all metadata for the `ListElement`

        Note
        ----
        Depending on the `level` specified in the `list` command, the returned dictionary contains
        all available keys from schema levels up to and including the requested level; keys that
        exist only at deeper levels are omitted.

        Examples
        --------
        >>> list_iterator = fdb.list(selection, level=3)
        >>> assert list_iterator

        >>> elements = list(list_iterator)

        >>> for element in elements:
        >>>     print(element.combined_key())

        Output:

        ``
        ...
        { 'class': 'ea', 'date': '20200104', ... , 'type': 'an' }
        ...
        ``
        """
        return self._element.combined_key()

    def keys(self) -> list[dict[str, str]]:
        """
        Return the MARS keys of the `ListElement` separated by their level in the schema.

        Returns
        -------
        `list[dict[str, str]]`
            List containing MARS keys and their values for the `ListElement`. The keys are split
            by their level in the schema

        Note
        ----
        Depending on the `level` specified in the `list` command, the returned dictionary contains
        all available keys from schema levels up to and including the requested level; keys that
        exist only at deeper levels are omitted.

        Examples
        --------
        >>> list_iterator = fdb.list(selection, level=3)
        >>> assert list_iterator

        >>> elements = list(list_iterator)

        >>> for element in elements:
        >>>     print(element.keys())

        Output:

        ``
        ...
        [{'class': 'ea', ... , 'time': '2100'}, {'levtype': 'sfc', 'type': 'an'}, {'param': '167', 'step': '0'}]
        ...
        ``
        """
        return self._element.keys()

    @property
    def data_handle(self) -> Optional[DataHandle]:
        """
        Access the `DataHandle`

        Returns
        -------
        `DataHandle`
            Data Handle for accessing the data of the list element

        Examples
        --------
        >>> data_handle = list_element.data_handle
        >>> if data_handle is not None:
        >>>     data_handle.open()
        >>>     data_handle.read(4)
        >>>     data_handle.close()

        Output:

        ``b"GRIB"``
        """
        if self.has_location():
            return DataHandle(self._element.data_handle(), _internal=True)

        logger.info(
            "Asking for data handle on list element without location. Did you specify `level=3` in a list call?"
        )

        return None

    @property
    def uri(self) -> Optional[URI]:
        """
        Access the URI of the list element

        Returns
        -------
        `URI`
            URI of the data

        Examples
        --------
        >>> uri = list_element.uri
        >>> print(uri)

        Output:

        ``<path/to/data_file>``
        """
        if self.has_location():
            return URI(self._element.uri())

        logger.info(
            "Asking for data handle on list element without location. Did you specify `level=3` in a list call?"
        )

        return None

    def __repr__(self) -> str:
        return str(self._element)


class WipeElementType(IntEnum):
    """

    Values
    ------
    - ERROR
    - CATALOGUE_INFO
    - CATALOGUE
    - CATALOGUE_INDEX
    - CATALOGUE_SAFE
    - CATALOGUE_CONTROL
    - STORE
    - STORE_AUX
    - STORE_SAFE
    - UNKNOWN
    """

    ERROR = 0
    CATALOGUE_INFO = auto()
    CATALOGUE = auto()
    CATALOGUE_INDEX = auto()
    CATALOGUE_SAFE = auto()
    CATALOGUE_CONTROL = auto()
    STORE = auto()
    STORE_AUX = auto()
    STORE_SAFE = auto()
    UNKNOWN = auto()

    @classmethod
    def _from_raw(cls, en: _WipeElementType):
        return WipeElementType[en.name]

    def _to_raw(self):
        return _WipeElementType[self.name]

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.__repr__()


class WipeElement:
    def __init__(self, wipe_element: str, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a WipeElement from user code is not supported.")
        self._element: _WipeElement = wipe_element

    def type(self) -> WipeElementType:
        return self._element.type()

    def msg(self) -> str:
        return self._element.msg()

    def uris(self) -> list[URI]:
        return [URI(uri) for uri in self._element.uris()]

    def __repr__(self) -> str:
        return str(self._element)


class StatusElement:
    def __init__(self, control_element: _ControlElement, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a StatusElement from user code is not supported.")
        self.element: _ControlElement = control_element

    def __eq__(self, other: object, /) -> bool:
        if isinstance(other, (StatusElement, ControlElement)):
            return (
                self.location() == other.location()
                and self.controlIdentifiers() == other.controlIdentifiers()
                and self.key() == other.key()
            )

        return False

    def __ne__(self, value: object, /) -> bool:
        # Needs to be implemented because of __eq__
        return not self.__eq__(value)

    def location(self) -> URI:
        return URI(self.element.location())

    def controlIdentifiers(self) -> list[ControlIdentifier]:
        return [ControlIdentifier._from_raw(el) for el in self.element.controlIdentifiers()]

    def key(self) -> MarsSelection:
        return self.element.key()

    def __repr__(self) -> str:
        return f"StatusElement(control_identifiers={self.controlIdentifiers()}, key={self.key()}, location={self.location()})"


class PurgeElement:
    def __init__(self, purge_element: str, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a PurgeElement from user code is not supported.")
        self.element: _PurgeElement = purge_element

    def __repr__(self) -> str:
        return str(self.element)


class StatsElement:
    def __init__(self, stats_element: _StatsElement, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a StatsElement from user code is not supported.")
        self.element: _StatsElement = stats_element

    def db_statistics(self) -> str:
        return self.element.db_statistics()

    def index_statistics(self) -> str:
        return self.element.index_statistics()

    def __repr__(self) -> str:
        return str(self.element)


class ControlElement:
    def __init__(self, control_element: _ControlElement, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a ControlElement from user code is not supported.")
        self.element: _ControlElement = control_element

    def __eq__(self, other: object, /) -> bool:
        if isinstance(other, (StatusElement, ControlElement)):
            return (
                self.location() == other.location()
                and self.controlIdentifiers() == other.controlIdentifiers()
                and self.key() == other.key()
            )

        return False

    def __ne__(self, value: object, /) -> bool:
        # Needs to be implemented because of __eq__
        return not self.__eq__(value)

    def location(self) -> URI:
        return URI(self.element.location())

    def controlIdentifiers(self) -> list[ControlIdentifier]:
        return [ControlIdentifier._from_raw(el) for el in self.element.controlIdentifiers()]

    def key(self) -> MarsSelection:
        return self.element.key()

    def __repr__(self) -> str:
        return f"ControlElement(control_identifiers={self.controlIdentifiers()}, key={self.key()}, location={self.location()})"


class IndexAxis(InternalMarsSelection):
    """
    `IndexAxis` class representing axes and their extent. The class implements all Dictionary
    functionalities. Key are the corresponding FDB keys (axes) and values are the values defining the extent
    of an axis.

    """

    def __init__(self, index_axis: _IndexAxis | MarsSelection) -> None:
        if isinstance(index_axis, _IndexAxis):
            self.index_axis: _IndexAxis = index_axis
        elif isinstance(index_axis, dict):
            internal_mars_selection = UserInputMapper.map_selection_to_internal(index_axis)
            self.index_axis: _IndexAxis = _IndexAxis(internal_mars_selection)
        else:
            raise TypeError(
                f"IndexAxis: Unknown type {type(index_axis)} for creating IndexAxis. Only `IndexAxis` or `dict[str, Collection[str]]` are allowed."
            )

    def __repr__(self) -> str:
        return repr(self.index_axis)

    def __getitem__(self, key: str) -> Collection[str]:
        return self.index_axis[key]

    def __setitem__(self, key, value):
        raise TypeError("This mapping is read-only")

    def __delitem__(self, key):
        raise TypeError("This mapping is read-only")

    def clear(self):
        raise TypeError("This mapping is read-only")

    def __len__(self) -> int:
        return len(self.index_axis)

    def __iter__(self) -> Iterator[str]:
        return iter(self.index_axis.keys())

    def __eq__(self, value) -> bool:
        dict_list_values = {}

        if isinstance(value, (dict, IndexAxis)):
            for k, v in value.items():
                if isinstance(v, str):
                    dict_list_values[k] = [v]
                elif not isinstance(v, Collection):
                    dict_list_values[k] = [str(v)]
                else:
                    dict_list_values[k] = v

            return dict(self.items()) == dict(dict_list_values.items())

        return False

    def __ne__(self, value: object, /) -> bool:
        # Needs to be implemented because of __eq__
        return not self.__eq__(value)

    def has_key(self, k) -> bool:
        return k in self.index_axis

    def keys(self) -> KeysView[str]:
        return KeysView(self)

    def values(self) -> ValuesView[Collection[str]]:
        return ValuesView(self)

    def items(self) -> ItemsView[str, Collection[str]]:
        return ItemsView(self)

    def __contains__(self, item: object):
        return item in self.index_axis
