import json
from pathlib import Path
from typing import Any, Collection, Dict, List, Mapping, Optional, Tuple, TypeAlias

from pyfdb_bindings import pyfdb_bindings as pyfdb_internal
import yaml

"""
Selection part of a MARS request.

This is a key-value map, with the data types allowed below
"""
MarsSelection: TypeAlias = Mapping[str, str | int | float | Collection[str | int | float]]

"""
This is the representation of a MARS identifier

This is a key-value List, mapping MARS keys to a string resembling a singluar value, see https://github.com/ecmwf/datacube-spec.
"""
MarsIdentifier = List[Tuple[str, str]] | Dict[str, str]


"""
This is the internal representation of a MARS selection

This is a key-value map, mapping MARS keys to a string resembling values, value lists or value ranges.
"""
InternalMarsSelection = Dict[str, Collection[str]]

"""
This is the internal representation of a MARS identifier

This is a key-value List, mapping MARS keys to a string resembling a singluar value, see https://github.com/ecmwf/datacube-spec.
"""
InternalMarsIdentifier = List[Tuple[str, str]]


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
        key_values=None,
        all: bool = False,
        minimum_key_set: List[str] | None = None,
    ) -> None:
        if key_values is not None:
            key_values = UserInputMapper.map_selection_to_internal(key_values)

        if minimum_key_set is None:
            minimum_key_set = []

        mars_request = MarsRequest("retrieve", key_values)

        self.tool_request = pyfdb_internal.FDBToolRequest(
            mars_request.request, all, minimum_key_set
        )

    @classmethod
    def from_mars_selection(cls, selection: InternalMarsSelection) -> "FDBToolRequest":
        if selection is None or len(selection) == 0:
            return cls(key_values=None, all=True)
        return cls(key_values=selection, all=False)

    def ____repr__(self) -> str:
        return str(self.tool_request)


class UserInputMapper:
    """
    Selection mapper for creating MARS selections.

    This class helps to create syntactically correctly structured MARS selections. If `strict_mode`
    is activated there will be checks whether keys have been set already.

    Examples
    --------
    TODO:
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
                converted_values = [
                    str(v) if isinstance(v, float) or isinstance(v, int) else v for v in values
                ]
                result[key] = converted_values
            # Values is a string or a float or an int
            elif isinstance(values, str) or isinstance(values, int) or isinstance(values, float):
                result[key] = [str(values)]
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
                converted_values = [
                    str(v) if isinstance(v, float) or isinstance(v, int) else v for v in values
                ]
                result[key] = converted_values
            # Values is a string or a float or an int
            elif isinstance(values, str) or isinstance(values, int) or isinstance(values, float):
                result[key] = str(values)
            else:
                raise ValueError(
                    f"Unknown type for key: {key}. Type must be int, float, str or a collection of those."
                )

        return result

    @classmethod
    def map_identifier_to_internal(cls, identifier: MarsIdentifier) -> InternalMarsIdentifier:
        key_values: Dict[str, str] = {}

        iterator = None

        if isinstance(identifier, List):
            iterator = identifier
        elif isinstance(identifier, Dict):
            iterator = identifier.items()
        else:
            raise ValueError(
                "Identifier: Unknown type for key_value_pairs. List[Tuple[str, str]] or Dict[str, str] needed"
            )

        for k, v in iterator:
            if k in key_values.keys():
                raise KeyError(
                    f"Identifier: Key {k} already exists in Identifier: {str(key_values)}"
                )

            if isinstance(v, list) or "/" in v:
                raise ValueError(
                    "No list of values allowed. An Identifier has to be a mapping from a single key to a single value."
                )
            else:
                key_values[k] = v

        return list(key_values.items())


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


class MarsRequest:
    def __init__(self, verb: str | None = None, key_values: None | MarsSelection = None) -> None:
        if key_values:
            key_values = UserInputMapper.map_selection_to_internal(key_values)

        if verb is None and key_values is None:
            self.request = pyfdb_internal.MarsRequest("retrieve")
        elif verb and key_values:
            self.request = pyfdb_internal.MarsRequest(verb, key_values)
        elif verb is not None:
            self.request = pyfdb_internal.MarsRequest(verb)
        elif verb or key_values:
            raise RuntimeError("MarsRequest: verb and key_values can only be specified together.")

    @classmethod
    def from_selection(cls, mars_selection: Optional[InternalMarsSelection]):
        if mars_selection is None:
            raise TypeError("MarsRequest: None type not allowed for a selection")
        result = MarsRequest()
        result.request = pyfdb_internal.MarsRequest("retrieve", mars_selection)
        return result

    def verb(self) -> str:
        return self.request.verb()

    # TODO(TKR): Check whether a property has memoized access
    def items(self) -> InternalMarsSelection:
        return self.request.key_values()

    def empty(self) -> bool:
        return self.request.empty()

    def __getitem__(self, key: str) -> str:
        return self.request[key]

    def __repr__(self) -> str:
        return str(self.request)

    def __len__(self) -> int:
        return len(self.request)
