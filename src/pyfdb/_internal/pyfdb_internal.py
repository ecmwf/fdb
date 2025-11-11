from typing import Any, Dict

from pyfdb_bindings import pyfdb_bindings as pyfdb_internal


MarsSelection = Dict[str, str]


def _flatten_values(key_values: Dict[str, Any]) -> Dict[str, str]:
    result = {}
    for k in key_values.keys():
        if isinstance(key_values[k], list):
            result[k] = "/".join(key_values[k])
        elif isinstance(key_values[k], str):
            result[k] = key_values[k]
        else:
            raise RuntimeError(
                f"MarsRequest: Value of unknown type. Expected str or list, found {type(key_values[k])}"
            )

    return result


class MarsRequest:
    def __init__(
        self, verb: str | None = None, key_values: MarsSelection | None = None
    ) -> None:
        if key_values:
            key_values = _flatten_values(key_values)

        if verb is None and key_values is None:
            self.request = pyfdb_internal.MarsRequest("retrieve")
        elif verb and key_values:
            self.request = pyfdb_internal.MarsRequest(verb, key_values)
        elif verb is not None:
            self.request = pyfdb_internal.MarsRequest(verb)
        elif verb or key_values:
            raise RuntimeError(
                "MarsRequest: verb and key_values can only be specified together."
            )

    @classmethod
    def from_selection(cls, mars_selection: MarsSelection):
        result = MarsRequest()
        result.request = pyfdb_internal.MarsRequest("retrieve", mars_selection)
        return result

    def verb(self) -> str:
        return self.request.verb()

    def key_values(self) -> dict[str, str]:
        return _flatten_values(self.request.key_values())

    def empty(self) -> bool:
        return self.request.empty()

    def __str__(self) -> str:
        return str(self.request)

    def __len__(self) -> int:
        return len(self.request)
