# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from collections.abc import Mapping
import logging
from typing import Collection, Dict, ItemsView, Iterator, KeysView, List, Sequence, ValuesView

from urllib.parse import ParseResult, urlparse

from pyfdb._internal import (
    ControlElement as _ControlElement,
)
from pyfdb._internal import (
    FileCopy as _FileCopy,
)
from pyfdb._internal import (
    IndexAxis as _IndexAxis,
)
from pyfdb._internal import (
    ListElement as _ListElement,
)
from pyfdb._internal import (
    StatsElement as _StatsElement,
)
from pyfdb._internal.pyfdb_internal import MarsSelection
from pyfdb.pyfdb_type import URI, DataHandle, ControlIdentifier


logger = logging.getLogger(__name__)


class ListElement:
    """Element returned from a listing command"""

    def __init__(self, list_element: _ListElement, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a ListElement from user code is not supported.")
        self._element: _ListElement = list_element

    @property
    def data_handle(self) -> DataHandle:
        """
        Access the DataHandle

        Returns
        -------
        `DataHandle`
            Data Handle for accessing the data of the list element

        Examples
        --------
        >>> data_handle = list_element.dataHandle()
        >>> data_handle.open()
        >>> data_handle.read(4)
        >>> data_handle.close()

        Output:

        ``b"GRIB"``
        """
        return DataHandle(self._element.data_handle(), _internal=True)

    @property
    def uri(self) -> URI | None:
        """
        Access the URI of the list element

        Returns
        -------
        `URI`
            URI of the data

        Examples
        --------
        >>> uri = list_element.uri()
        >>> print(uri)

        Output:

        ``<path/to/data_file>``
        """
        try:
            return URI(self._element.uri())
        except RuntimeError as _:
            logger.info(
                "Couldn't find an URI for the given element. Did you specify the `level=3` of the list call correctly?"
            )
            return None

    def __repr__(self) -> str:
        return str(self._element)


class WipeElement:
    def __init__(self, wipe_element: str, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a WipeElement from user code is not supported.")
        self.element: str = wipe_element

    def __repr__(self) -> str:
        return str(self.element)


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

    def location(self) -> URI:
        return URI(self.element.location())

    def controlIdentifiers(self) -> List[ControlIdentifier]:
        return [ControlIdentifier._from_raw(el) for el in self.element.controlIdentifiers()]

    def key(self) -> MarsSelection:
        return self.element.key()

    def __repr__(self) -> str:
        return f"StatusElement(control_identifiers={self.controlIdentifiers()}, key={self.key()}, location={self.location()})"


class MoveElement:
    def __init__(self, file_copy: _FileCopy, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a MoveElement from user code is not supported.")
        self.element: _FileCopy = file_copy

    def execute(self):
        """
        Method for executing a `MoveElement`. This triggers the move of the associated data.
        """
        if self.element is not None:
            self.element.execute()

    def cleanup(self):
        if self.element is not None:
            self.element.cleanup()

    def __repr__(self) -> str:
        return str(self.element)


class PurgeElement:
    def __init__(self, purge_element: str, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a PurgeElement from user code is not supported.")
        self.element: str = purge_element

    def __repr__(self) -> str:
        return str(self.element)


class StatsElement:
    def __init__(self, stats_element: _StatsElement, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a StatsElement from user code is not supported.")
        self.element: _StatsElement = stats_element

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

    def location(self) -> URI:
        return URI(self.element.location())

    def controlIdentifiers(self) -> List[ControlIdentifier]:
        return [ControlIdentifier._from_raw(el) for el in self.element.controlIdentifiers()]

    def key(self) -> MarsSelection:
        return self.element.key()

    def __repr__(self) -> str:
        return f"ControlElement(control_identifiers={self.controlIdentifiers()}, key={self.key()}, location={self.location()})"


class IndexAxis(Mapping[str, Sequence[str]]):
    """
    `IndexAxis` class representing axes and their extent. The class implements all Dictionary
    functionalities. Key are the corresponding FDB keys (axes) and values are the values defining the extent
    of an axis.

    """

    def __init__(self, index_axis: _IndexAxis, *, _internal=False) -> None:
        if not _internal:
            raise TypeError("Creating a IndexAxis from user code is not supported.")
        self.index_axis: _IndexAxis = index_axis

    def __repr__(self) -> str:
        return str(self.index_axis)

    def __getitem__(self, key: str) -> str:
        values = self.index_axis[key]
        return values

    def __len__(self) -> int:
        return len(self.index_axis)

    def __iter__(self) -> Iterator[str]:
        return iter(self.index_axis.keys())

    def __eq__(self, value) -> bool:
        dict_list_values = {}

        if isinstance(value, Dict) or isinstance(value, IndexAxis):
            for k, v in value.items():
                if isinstance(v, str):
                    dict_list_values[k] = [v]
                elif not isinstance(v, Collection):
                    dict_list_values[k] = [v]
                else:
                    dict_list_values[k] = v

            return dict(self.items()) == dict(dict_list_values.items())
        else:
            return False

    def has_key(self, k) -> bool:
        return k in self.index_axis

    def keys(self) -> KeysView[str]:
        return self.index_axis.keys()

    def values(self) -> ValuesView[str]:
        return self.index_axis.values()

    def items(self) -> ItemsView[str, Sequence[str]]:
        return self.index_axis.items()

    def __contains__(self, item: object):
        return item in self.index_axis
