import json
from typing import Any, List, Tuple

from pychunked_data_view import AxisDefinition, ExtractorType


def to_MARS_selection_str(dict) -> str:
    result = ""
    for k, values in dict.items():
        v_str = ""
        if isinstance(values, list):
            v_str = "/".join([str(v) for v in values])
        elif not isinstance(values, str):
            v_str = str(values)
        else:
            v_str = values

        result += f"{k}={v_str},"

    # Remove last comma
    result = result[:-1]

    return result


class Request:
    def __init__(
        self, request: str, axes: list[Tuple[list[str], bool]], extractor_type: str
    ) -> None:
        self.request = request
        self.axes = [AxisDefinition(listing, b) for (listing, b) in axes]
        self.extractor = ExtractorType[extractor_type]


class RequestDTO:
    def __init__(
        self,
        request: dict[str, Any] | str,
        axes: list[Tuple[list[str], bool]],
        extractor_type: ExtractorType,
    ) -> None:
        if isinstance(request, str):
            self.request = request
        else:
            self.request = to_MARS_selection_str(request)
        self.axes = axes
        self.extractor = extractor_type.name

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=2)


class RequestsDTO:
    def __init__(self, requests: list[RequestDTO]) -> None:
        self.requests = requests

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=2)


class RequestDTOMapper:
    @classmethod
    def map_dto(cls, request_dto: RequestDTO) -> Request:
        return Request(request_dto.request, request_dto.axes, request_dto.extractor)

    @classmethod
    def map_json(cls, json) -> List[Request]:
        request_dtos = [
            RequestDTO(
                request=request_json["request"],
                axes=request_json["axes"],
                extractor_type=ExtractorType[request_json["extractor"]],
            )
            for request_json in json["requests"]
        ]

        return [RequestDTOMapper.map_dto(r_dto) for r_dto in request_dtos]
