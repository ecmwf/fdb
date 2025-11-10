# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from typing import List

from pyfdb import URI, DataHandle

from pyfdb._internal import (
    ListElement as _ListElement,
    StatsElement as _StatsElement,
    ControlElement as _ControlElement,
    IndexAxis as _IndexAxis,
    FileCopy as _FileCopy,
)


class ListElement:
    """Element returned from a listing command"""

    def __init__(self) -> None:
        self._element: _ListElement
        raise NotImplementedError(
            "Read-only class. Should be construced using the _from_raw method"
        )

    def __new__(cls) -> "ListElement":
        bare_instance = object.__new__(cls)
        return bare_instance

    @classmethod
    def _from_raw(cls, list_element: _ListElement) -> "ListElement":
        """
        Internal method for generating a `ListElement` from the PyBind11 exposed element

        Parameters
        ----------
        `list_element` : `pyfdb._interal.ListElement`
            Internal list element

        Returns
        -------
        `ListElement`
            User facing list element
        """
        result = cls.__new__(cls)
        result._element = list_element
        return result

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
        >>> data_handle.read(4)

        ```
        b"GRIB"
        ```
        """
        return DataHandle._from_raw(self._element.data_handle())

    def uri(self) -> URI:
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

        ```
        <path/to/data_file>
        ```
        """
        return URI._from_raw(self._element.uri())

    def __str__(self) -> str:
        return str(self._element)


class WipeElement:
    def __init__(self) -> None:
        self.element: str
        raise NotImplementedError(
            "Read-only class. Should be construced using the _from_raw method"
        )

    def __new__(cls) -> "WipeElement":
        bare_instance = object.__new__(cls)
        return bare_instance

    @classmethod
    def _from_raw(cls, wipe_element: str) -> "WipeElement":
        """
        Internal method for generating a `WipeElement` from the PyBind11 exposed element

        Parameters
        ----------
        `wipe_element` : `pyfdb._interal.WipeElement`
            Internal wipe element

        Returns
        -------
        `WipeElement`
            User facing wipe element
        """
        result = cls.__new__(cls)
        result.element = wipe_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class StatusElement:
    def __init__(self) -> None:
        self.element: _ControlElement
        raise NotImplementedError(
            "Read-only class. Should be construced using the _from_raw method"
        )

    def __new__(cls) -> "StatusElement":
        bare_instance = object.__new__(cls)
        return bare_instance

    @classmethod
    def _from_raw(cls, status_element: _ControlElement) -> "StatusElement":
        """
        Internal method for generating a `StatusElement` from the PyBind11 exposed element

        Parameters
        ----------
        `status_element` : `pyfdb._interal.StatusElement`
            Internal status element

        Returns
        -------
        `StatusElement`
            User facing status element
        """
        result = cls.__new__(cls)
        result.element = status_element
        return result

    def __eq__(self, other: object, /) -> bool:
        if isinstance(other, StatusElement):
            return str(self.element) == str(other.element)

        return False

    def __str__(self) -> str:
        return str(self.element)


class MoveElement:
    def __init__(self) -> None:
        self.element: _FileCopy
        raise NotImplementedError(
            "Read-only class. Should be construced using the _from_raw method"
        )

    def __new__(cls) -> "MoveElement":
        bare_instance = object.__new__(cls)
        return bare_instance

    @classmethod
    def _from_raw(cls, move_element: str) -> "MoveElement":
        """
        Internal method for generating a `MoveElement` from the PyBind11 exposed element

        Parameters
        ----------
        `move_element` : `pyfdb._interal.MoveElement`
            Internal move element

        Returns
        -------
        `MoveElement`
            User facing move element
        """
        result = cls.__new__(cls)
        result.element = move_element
        return result

    def execute(self):
        """
        Method for executing a `MoveElement`. This triggers the move of the associated data.
        """
        if self.element is not None:
            self.element.execute()

    def __str__(self) -> str:
        return str(self.element)


class PurgeElement:
    def __init__(self) -> None:
        self.element: str
        raise NotImplementedError(
            "Read-only class. Should be construced using the _from_raw method"
        )

    @classmethod
    def _from_raw(cls, purge_element: str) -> "PurgeElement":
        """
        Internal method for generating a `PurgeElement` from the PyBind11 exposed element

        Parameters
        ----------
        `purge_element` : `pyfdb._interal.PurgeElement`
            Internal purge element

        Returns
        -------
        `PurgeElement`
            User facing purge element
        """
        result = cls.__new__(cls)
        result.element = purge_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class StatsElement:
    def __init__(self) -> None:
        self.element: _StatsElement
        raise NotImplementedError(
            "Read-only class. Should be construced using the _from_raw method"
        )

    def __new__(cls) -> "StatsElement":
        bare_instance = object.__new__(StatsElement)
        return bare_instance

    @classmethod
    def _from_raw(cls, stats_element: _StatsElement) -> "StatsElement":
        """
        Internal method for generating a `StatsElement` from the PyBind11 exposed element

        Parameters
        ----------
        `stats_element` : `pyfdb._interal.StatsElement`
            Internal stats element

        Returns
        -------
        `StatsElement`
            User facing stats element
        """
        result = cls.__new__(cls)
        result.element = stats_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class ControlElement:
    def __init__(self) -> None:
        self.element = _ControlElement
        raise NotImplementedError(
            "Read-only class. Should be construced using the _from_raw method"
        )

    def __new__(cls) -> "ControlElement":
        bare_instance = object.__new__(cls)
        return bare_instance

    @classmethod
    def _from_raw(cls, control_element: _ControlElement) -> "ControlElement":
        """
        Internal method for generating a `ControlElement` from the PyBind11 exposed element

        Parameters
        ----------
        `control_element` : `pyfdb._interal.ControlElement`
            Internal control element

        Returns
        -------
        `ControlElement`
            User facing control element
        """
        result = cls.__new__(cls)
        result.element = control_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class IndexAxis:
    """
    `IndexAxis` class respresenting axes and their extent. The class implements all Dictionary
    functionalities. Key are the corresponding FDB keys (axes) and values are the values defining the extent
    of an axis.

    """

    def __init__(self) -> None:
        self.index_axis = _IndexAxis
        raise NotImplementedError(
            "Read-only class. Should be construced using the _from_raw method"
        )

    def __new__(cls) -> "IndexAxis":
        bare_instance = object.__new__(cls)
        return bare_instance

    @classmethod
    def _from_raw(cls, index_axis: _IndexAxis) -> "IndexAxis":
        """
        Internal method for generating an `IndexAxis` from the PyBind11 exposed element

        Parameters
        ----------
        `index_axis` : `pyfdb._interal.IndexAxis`
            Internal IndexAxis element

        Returns
        -------
        `IndexAxis`
            User facing IndexAxis element
        """
        result = cls.__new__(cls)
        result.index_axis = index_axis
        return result

    def __str__(self) -> str:
        return str(self.index_axis)

    def __setitem__(self, key, item):
        raise AttributeError("IndexAxis class is read only.")

    def __getitem__(self, key) -> List[str]:
        return self.index_axis[key]

    def __len__(self) -> int:
        return len(self.index_axis)

    def __delitem__(self, key):
        raise AttributeError("IndexAxis class is read only.")

    def clear(self):
        raise RuntimeError("IndexAxis class is read only.")

    def copy(self):
        raise RuntimeError("IndexAxis is non-copyable.")

    def has_key(self, k) -> bool:
        return k in self.index_axis

    def keys(self) -> List[str]:
        return self.index_axis.keys()

    def values(self):
        return self.index_axis.values()

    def items(self):
        return self.index_axis.items()

    def __contains__(self, item: str):
        return item in self.index_axis
