import json
from pathlib import Path
from typing import Any, Collection, Dict, List, Optional, Tuple

import yaml

from pyfdb_bindings import pyfdb_bindings as pyfdb_internal


# Initial setup of binding via eckit main
pyfdb_internal.init_bindings()

InternalMarsSelection = Dict[str, Collection[str]]
"""
This is the internal representation of a MARS selection

This is a key-value map, mapping MARS keys to a string resembling values, value lists or value ranges.
"""

InternalMarsIdentifier = List[Tuple[str, str]]
"""
This is the internal representation of a MARS identifier

This is a key-value List, mapping MARS keys to a string resembling a singluar value, see https://github.com/ecmwf/datacube-spec.
"""


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
    ...     {
    ...         "class": "ea",
    ...         "domain": "g",
    ...         "expver": "0001",
    ...         "stream": "oper",
    ...         "date": "20200101",
    ...         "time": "1800",
    ...     },
    ...     # all = False,
    ...     # minimum_key_set = None,
    ... )
    """

    def __init__(
        self,
        key_values=None | InternalMarsSelection,
        all: bool = False,
        minimum_key_set: List[str] | None = None,
    ) -> None:
        if key_values is not None:
            key_values = key_values

        if minimum_key_set is None:
            minimum_key_set = []

        self.tool_request = pyfdb_internal.FDBToolRequest(key_values, all, minimum_key_set)

    @classmethod
    def from_internal_mars_selection(
        cls, selection: InternalMarsSelection | None
    ) -> "FDBToolRequest":
        if selection is None or len(selection) == 0:
            return cls(key_values={}, all=True)

        for k, v in selection.items():
            if isinstance(v, str):
                raise ValueError(f"Expecting collection of values for key {k}, not {v}")

        return cls(key_values=selection, all=False)

    def __repr__(self) -> str:
        return repr(self.tool_request)


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
        For usage examples see the comments of FDB constructor.

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
        ...     {
        ...         "type":"local",
        ...         "engine":"toc",
        ...         "schema": "<schema_path>",
        ...         "spaces":[
        ...             {
        ...                 "handler":"Default",
        ...                 "roots":[
        ...                     {"path": "<db_store_path>"},
        ...                 ],
        ...             }
        ...         ],
        ...     })
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
            raise ValueError("Config: Unknown config type, must be str, dict or Path.")

        return json.dumps(config_result)

    @classmethod
    def from_json(cls, config: str) -> Dict[str, Any]:
        return json.loads(config)
