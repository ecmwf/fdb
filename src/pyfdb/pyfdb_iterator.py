from typing import Dict, List, Tuple
from pyfdb import DataHandle

from pyfdb import URI

from pyfdb_bindings import pyfdb_bindings as pyfdb_internal


class ListElement:
    def __init__(self) -> None:
        self._element = None

    @classmethod
    def _from_raw(cls, list_element: pyfdb_internal.ListElement):
        result = ListElement()
        result._element = list_element
        return result

    def dataHandle(self):
        return DataHandle(self._element.location().dataHandle())

    def uri(self):
        return URI._from_raw(self._element.uri())

    def __str__(self) -> str:
        return str(self._element)


class WipeElement:
    def __init__(self) -> None:
        self.element: str | None = None

    @classmethod
    def _from_raw(cls, wipe_element: str):
        result = WipeElement()
        result.element = wipe_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class DumpElement:
    def __init__(self) -> None:
        self.element: str | None = None

    @classmethod
    def _from_raw(cls, dump_element: str):
        result = DumpElement()
        result.element = dump_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class StatusElement:
    def __init__(self) -> None:
        self.element: pyfdb_internal.StatusElement | None = None

    @classmethod
    def _from_raw(cls, status_element: str):
        result = StatusElement()
        result.element = status_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class MoveElement:
    def __init__(self) -> None:
        self.element: pyfdb_internal.FileCopy | None = None

    @classmethod
    def _from_raw(cls, move_element: str):
        result = MoveElement()
        result.element = move_element
        return result

    def execute(self):
        if self.element is not None:
            self.element.execute()

    def __str__(self) -> str:
        return str(self.element)


class PurgeElement:
    def __init__(self) -> None:
        self.element: str | None = None

    @classmethod
    def _from_raw(cls, purge_element: str):
        result = PurgeElement()
        result.element = purge_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class StatsElement:
    def __init__(self) -> None:
        self.element: pyfdb_internal.StatsElement | None = None

    @classmethod
    def _from_raw(cls, stats_element: pyfdb_internal.StatsElement):
        result = StatusElement()
        result.element = stats_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class ControlElement:
    def __init__(self) -> None:
        self.element: pyfdb_internal.ControlElement | None = None

    @classmethod
    def _from_raw(cls, control_element: pyfdb_internal.ControlElement):
        result = ControlElement()
        result.element = control_element
        return result

    def __str__(self) -> str:
        return str(self.element)


class IndexAxis:
    def __init__(self) -> None:
        self.index_axis: pyfdb_internal.IndexAxis | None = None

    @classmethod
    def _from_raw(cls, index_axis: pyfdb_internal.IndexAxis):
        result = IndexAxis()
        result.index_axis = index_axis
        return result

    def map(self) -> Dict[str, List[str]]:
        return self.index_axis.map()

    def __str__(self) -> str:
        return str(self.index_axis)

    def __setitem__(self, key, item):
        raise AttributeError("IndexAxis class is read only.")

    def __getitem__(self, key) -> List[str]:
        return self.index_axis[key]

    def __len__(self) -> int:
        if self.index_axis is not None:
            return len(self.index_axis)
        else:
            return 0

    def __delitem__(self, key):
        raise AttributeError("IndexAxis class is read only.")

    def clear(self):
        raise RuntimeError("IndexAxis class is read only.")

    def copy(self):
        raise RuntimeError("IndexAxis is non-copyable.")

    def has_key(self, k) -> bool:
        return k in self.__dict__

    def keys(self) -> List[str]:
        if self.index_axis is None:
            return []
        return self.index_axis.keys()

    def values(self):
        return self.index_axis.values()

    def items(self):
        return self.index_axis.items()

    def __contains__(self, item: str):
        return item in self.index_axis
