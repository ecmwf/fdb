import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyfdb_bindings import pyfdb_bindings as pyfdb_internal
import yaml


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
        key_values=None,
        all: bool = False,
        minimum_key_set: List[str] | None = None,
    ) -> None:
        if key_values is not None:
            key_values = _flatten_values(key_values)

        if minimum_key_set is None:
            minimum_key_set = []

        mars_request = MarsRequest("retrieve", key_values)

        self.tool_request = pyfdb_internal.FDBToolRequest(
            mars_request.request, all, minimum_key_set
        )

    @classmethod
    def from_mars_selection(cls, selection):
        from pyfdb import WildcardMarsSelection

        return cls(
            key_values=selection, all=isinstance(selection, WildcardMarsSelection)
        )

    def __str__(self) -> str:
        return str(self.tool_request)


class ConfigMapper:
    @classmethod
    def to_json(cls, config: str | dict | Path | None) -> Optional[str]:
        """
        Mapper for creating a configuration string from given config

        Parameters
        ----------
        `config`: `str` | `dict` | `Path`
                System or user configuration

        Note
        ----
        *In case you want to supply a config to a FDB, hand in the needed arguments to the FDB directly.*
        For usage examples see the comments of PyFDB constructor.

        The config parameter can have different types, depending on the meaning:
            - `str` is used as a yaml representation to parse the config
            - `dict` is interpreted as hierarchical format to represent a config, see example
            - `Path` is interpreted as a location of the config and read as a YAML file

        Returns
        -------
        JSON string representing the given FDB configuration.

        Examples
        --------
        >>> config = ConfigMapper.to_json(
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

        if config is None:
            return config

        config_result = None

        if isinstance(config, Path):
            config_result = yaml.safe_load(config.read_text())
        elif isinstance(config, dict):
            config_result = config
        elif isinstance(config, str):
            config_result = yaml.safe_load(config)
        else:
            raise RuntimeError(
                "Config: Unknown config type, must be str, dict or Path."
            )

        return json.dumps(config_result)


class MarsRequest:
    def __init__(self, verb: str | None = None, key_values: None = None) -> None:
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
    def from_selection(cls, mars_selection):
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
