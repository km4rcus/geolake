from typing import Optional, List, Dict, Union

from pydantic import BaseModel, root_validator

class GeoQuery(BaseModel):
    variable: List[str]
    time: Optional[Union[Dict[str, str], Dict[str, List[str]]]]
    area: Optional[Dict[str, float]]
    locations: Optional[Dict[str, List[float]]]
    vertical: Optional[Union[float, List[float]]]
    filters: Optional[Dict]

    @root_validator
    def area_locations_mutually_exclusive_validator(cls, query):
        if query["area"] is not None and query["locations"] is not None:
            raise KeyError("area and locations couldn't be processed together, please use one of them")
        return query
