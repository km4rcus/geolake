import json
from typing import Optional, List, Dict, Union, Mapping, Any

from pydantic import BaseModel, root_validator


class GeoQuery(BaseModel, extra="allow"):
    variable: Optional[Union[str, List[str]]]
    # TODO: Check how `time` is to be represented
    time: Optional[Union[Dict[str, str], Dict[str, List[str]]]]
    area: Optional[Dict[str, float]]
    location: Optional[Dict[str, Union[float, List[float]]]]
    vertical: Optional[Union[float, List[float]]]
    filters: Optional[Dict]

    # TODO: Check if we are going to allow the vertical coordinates inside both
    # `area`/`location` nad `vertical`

    @root_validator
    def area_locations_mutually_exclusive_validator(cls, query):
        if query["area"] is not None and query["location"] is not None:
            raise KeyError(
                "area and location couldn't be processed together, please use"
                " one of them"
            )
        return query

    @root_validator(pre=True)
    def build_filters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        filters = {k: v for k, v in values.items() if k not in cls.__fields__}
        values = {k: v for k, v in values.items() if k in cls.__fields__}
        values["filters"] = filters
        return values

    def original_query_json(self):
        """Return the JSON representation of the original query submitted
        to the geokube-dds"""
        res = super().dict()
        return json.dumps(dict(**res.pop("filters", {}), **res))
