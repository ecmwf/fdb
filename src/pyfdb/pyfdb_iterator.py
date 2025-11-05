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
    def _from_raw(cls, dump_element: str):
        result = StatusElement()
        result.element = dump_element
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
