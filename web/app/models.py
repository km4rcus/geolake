"""Module containing utils classes for view data for the Webportal"""
import os
import json
import logging
from enum import Enum
from typing import Any, ClassVar, Optional, Union
from datetime import date, datetime, timedelta

from pydantic import BaseModel, AnyHttpUrl, HttpUrl, validator, root_validator
from db.dbmanager.dbmanager import RequestStatus


class RequestStatusDTO(Enum):
    """DTO enum for request statuses"""

    Completed = "DONE"
    Running = "RUNNING"
    Pending = "PENDING"
    Failed = "FAILED"


class Contact(BaseModel):
    """Contact DTO of dataset metadata"""

    name: str
    email: str
    webpage: str  # some products have wrong webpage, HttpUrl


class License(BaseModel):
    """License DTO of dataset metadata"""

    name: str
    url: Optional[
        str
    ] = None  # some products have wrong webpage, Optional[HttpUrl] = None


class ProdId(BaseModel):
    """Product short DTO of dataset metadata"""

    id: str
    description: Optional[str] = ""

    @root_validator(pre=True)
    def preprocess_load(cls, values):
        if "description" not in values:
            values["description"] = values["id"]
        return values


class DatasetMetadata(BaseModel):
    """Dataset metadata DTO with information about name, default product, description, etc."""

    id: str
    default: Optional[str] = None
    description: Optional[str] = ""
    label: Optional[str] = ""
    how_to_cite: Optional[str] = ""
    image: Optional[HttpUrl] = ""
    attribution: Optional[str] = ""
    update_frequency: Optional[str] = ""
    doi: Optional[HttpUrl] = None
    publication_date: Optional[date] = None
    contact: Contact
    license: Optional[License] = None
    products: list[ProdId]

    @validator("publication_date", pre=True)
    def parse_publication_date(cls, value):
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%d").date()
        return value

    @root_validator(pre=True)
    def preprocess_products(cls, values):
        prods = [
            {"id": prod_key, "description": prod.get("description")}
            for prod_key, prod in values["products"].items()
        ]
        return dict(products=prods, **values["metadata"])


class ListOfDatasets(BaseModel):
    """List of datasets DTO representing output for /datasets request"""

    version: Optional[str] = "v1"
    status: Optional[str] = "OK"
    data: list[DatasetMetadata]

    @classmethod
    def from_details(cls, details):
        return cls(data=details)


class Filter(BaseModel):
    """Filter DTO of product metadata"""

    name: Optional[str] = None
    user_defined: Optional[bool] = False
    label: Optional[str] = None

    @root_validator
    def match_label(cls, values):
        if values["label"] is None:
            values["label"] = values["name"]
        return values

    @validator("user_defined", pre=True)
    def maybe_cast_user_defined(cls, value):
        if isinstance(value, str):
            return value.lower() in ["t", "true", "yes"]
        elif isinstance(value, bool):
            return value
        else:
            raise TypeError


class Domain(BaseModel):
    """Domain DTO of the kube. It contains cooridnate reference system and coordinates"""

    crs: dict[str, Any]
    coordinates: dict[str, Any]


class Field(BaseModel):
    """Single field DTO of the kube"""

    name: str
    description: Optional[str] = None

    @root_validator
    def match_description(cls, values):
        if values["description"] is None:
            values["description"] = values["name"]
        return values


class Kube(BaseModel):
    """Single Kube DTO - a domain and a list of fields"""

    domain: Domain
    fields: list[Field]

    @validator("fields", pre=True)
    def parse_field(cls, value):
        return [
            {"name": fieild_key, "description": field.get("description")}
            for fieild_key, field in value.items()
        ]


class DatasetRow(BaseModel):
    """DTO contatining attributes and associated datacube"""

    attributes: dict[str, str]
    datacube: Kube


class ProductMetadata(BaseModel):
    """Product metadata DTO"""

    catalog_dir: str
    filters: Optional[dict[str, Filter]] = None
    role: Optional[str] = "public"

    @validator("filters", pre=True)
    def match_filters(cls, filters):
        if isinstance(filters, dict):
            return filters
        if isinstance(filters, list):
            return {item["name"]: item for item in filters}
        raise TypeError

    @validator("role")
    def match_role(cls, role):
        assert role in ["public", "admin", "internal"]
        return role


class Product(BaseModel):
    """Product DTO"""

    _SUPPORTED_FORMATS_LABELS: ClassVar[dict[str, str]] = {
        "grib": "GRIB",
        "pickle": "PICKLE",
        "netcdf": "netCDF",
        "geotiff": "geoTIFF",
    }
    id: str
    data: list[Union[DatasetRow, Kube]]
    metadata: ProductMetadata
    description: Optional[str] = None
    dataset: DatasetMetadata

    @validator("data", pre=True)
    def match_data_list(cls, value):
        if not isinstance(value, list):
            return [value]
        return value

    @validator("description", always=True)
    def match_description(cls, value, values):
        if value is None:
            return values["id"]
        return value


class WidgetsCollection(BaseModel):
    """DTO including all information required by the Web Portal to render datasets"""

    version: Optional[str] = "v1"
    status: Optional[str] = "OK"
    id: str
    label: str
    dataset: DatasetMetadata
    widgets: list[dict]
    widgets_order: list[str]


class Request(BaseModel):
    """Single request DTO for Web portal"""

    request_id: str
    dataset: str
    product: str
    request_json: dict
    submission_date: datetime
    end_date: Optional[datetime] = None
    duration: Optional[timedelta] = None
    size: Optional[int] = None
    url: Optional[str] = None
    status: str

    @root_validator(pre=True)
    def match_keys(cls, values):
        values["request_json"] = json.loads(values.pop("query", "{}"))
        values["submission_date"] = values.pop("created_on", None)
        values["status"] = RequestStatusDTO(
            RequestStatus(values["status"]).name
        ).name
        if download := values.get("download"):
            values["url"] = download.get("download_uri")
            values["end_date"] = download.get("created_on")
            values["size"] = download.get("size_bytes")
        return values

    @validator("duration", pre=True)
    def match_duration(cls, value, values):
        # TODO: fix, duration is always null in resulting JSON
        if last_update := values.get("end_date"):
            return last_update - values["submission_date"]
        return value

    def add_url_prefix(self, prefix):
        """Add inplace prefix to the URL in the following way:
            resulting url = prefix + base url

        Parameters
        -------
        prefix : str
            Prefix to add to the URL
        """
        self.url = "".join([prefix, self.url])


class ListOfRequests(BaseModel):
    """DTO for list of requests"""

    version: Optional[str] = "v1"
    status: Optional[str] = "OK"
    data: Optional[list[Request]]

    def add_requests_url_prefix(self, prefix: str):
        """Add inplace prefix to URL of each Request in 'data' attribute
        by calling Request.add_url_prefix method

        Parameters
        -------
        prefix : str
            Prefix to add to all Requests URL
        """
        for req in self.data:
            req.add_url_prefix(prefix)
