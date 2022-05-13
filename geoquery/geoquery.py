from typing import Optional, List, Dict, Union

from pydantic import BaseModel, root_validator


class GeoQuery(BaseModel):
    variable: Optional[Union[str, List[str]]]
    # TODO: Check how `time` is to be represented
    time: Optional[Union[Dict[str, str], Dict[str, List[str]]]]
    area: Optional[Dict[str, float]]
    locations: Optional[Dict[str, Union[float, List[float]]]]
    vertical: Optional[Union[float, List[float]]]
    # TODO: Check if `filters` are going to be dropped
    filters: Optional[Dict]

    # TODO: Check if we are going to allow the vertical coordinates inside both
    # `area`/`locations` nad `vertical`

    @root_validator
    def area_locations_mutually_exclusive_validator(cls, query):
        if query["area"] is not None and query["locations"] is not None:
            raise KeyError(
                "area and locations couldn't be processed together, please use"
                " one of them"
            )
        return query
