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


class ListIterator:
    def __init__(self) -> None:
        self._list_iterator = None

    @classmethod
    def _from_raw(cls, list_iterator: pyfdb_internal.ListIterator):
        result = ListIterator()
        result._list_iterator = list_iterator
        return result

    def __iter__(self):
        return self

    def __next__(self):
        if not self._list_iterator:
            raise StopIteration

        return ListElement._from_raw(self._list_iterator.next())


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


class WipeIterator:
    def __init__(self) -> None:
        self._iterator: pyfdb_internal.StringApiIterator | None = None

    @classmethod
    def _from_raw(cls, iterator: pyfdb_internal.StringApiIterator):
        result = WipeIterator()
        result._iterator = iterator
        return result

    def __iter__(self):
        return self

    def __next__(self):
        if not self._iterator:
            raise StopIteration

        return WipeElement._from_raw(self._iterator.next())


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


class DumpIterator:
    def __init__(self) -> None:
        self._dump_iterator: pyfdb_internal.StringApiIterator | None = None

    @classmethod
    def _from_raw(cls, dump_iterator: pyfdb_internal.StringApiIterator):
        result = DumpIterator()
        result._dump_iterator = dump_iterator
        return result

    def __iter__(self):
        return self

    def __next__(self):
        if not self._dump_iterator:
            raise StopIteration

        return DumpElement._from_raw(self._dump_iterator.next())


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


class StatusIterator:
    def __init__(self) -> None:
        self._status_iterator: pyfdb_internal.ControlApiIterator | None = None

    @classmethod
    def _from_raw(cls, status_iterator: pyfdb_internal.ControlApiIterator):
        result = StatusIterator()
        result._status_iterator = status_iterator
        return result

    def __iter__(self):
        return self

    def __next__(self):
        if not self._status_iterator:
            raise StopIteration

        return StatusElement._from_raw(self._status_iterator.next())


class MoveElement:
    def __init__(self) -> None:
        self.element: pyfdb_internal.FileCopy | None = None

    @classmethod
    def _from_raw(cls, dump_element: str):
        result = MoveElement()
        result.element = dump_element
        return result

    def execute(self):
        if self.element is not None:
            self.element.execute()

    def __str__(self) -> str:
        return str(self.element)


class MoveIterator:
    def __init__(self) -> None:
        self._iterator: pyfdb_internal.FileCopyApiIterator | None = None

    @classmethod
    def _from_raw(cls, move_iterator: pyfdb_internal.FileCopyApiIterator):
        result = MoveIterator()
        result._iterator = move_iterator
        return result

    def __iter__(self):
        return self

    def __next__(self):
        if not self._iterator:
            raise StopIteration

        return MoveElement._from_raw(self._iterator.next())


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


class PurgeIterator:
    def __init__(self) -> None:
        self._iterator: pyfdb_internal.StringApiIterator | None = None

    @classmethod
    def _from_raw(cls, purge_iterator: pyfdb_internal.StringApiIterator):
        result = PurgeIterator()
        result._iterator = purge_iterator
        return result

    def __iter__(self):
        return self

    def __next__(self):
        if not self._iterator:
            raise StopIteration

        return PurgeElement._from_raw(self._iterator.next())
