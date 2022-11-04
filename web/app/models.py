"""Module containing utils classes for view data for the Webportal"""
import os
import logging
from typing import Any, ClassVar, Optional
from datetime import date, datetime

from pydantic import BaseModel, HttpUrl, validator, root_validator


from .util import log_execution_time


class Contact(BaseModel):
    name: str
    email: str
    webpage: str  # some products have wrong webpage, HttpUrl


class License(BaseModel):
    name: str
    url: Optional[
        str
    ] = None  # some products have wrong webpage, Optional[HttpUrl] = None


class ProdId(BaseModel):
    id: str
    description: Optional[str] = ""

    @root_validator(pre=True)
    def preprocess_load(cls, values):
        if "description" not in values:
            values["description"] = values["id"]
        return values


class DatasetMetadata(BaseModel):
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
    _LOG: ClassVar[logging.Logger] = logging.getLogger("Converter")

    version: Optional[str] = "v1"
    status: Optional[str] = "OK"
    data: list[DatasetMetadata]

    @classmethod
    @log_execution_time(_LOG)
    def from_details(cls, details):
        return cls(data=details)


class Filter(BaseModel):
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
    crs: dict[str, Any]
    coordinates: dict[str, Any]


class Field(BaseModel):
    name: str
    description: Optional[str] = None

    @root_validator
    def match_description(cls, values):
        if values["description"] is None:
            values["description"] = values["name"]
        return values


class Kube(BaseModel):
    domain: Domain
    fields: list[Field]

    @validator("fields", pre=True)
    def parse_field(cls, value):
        return [
            {"name": fieild_key, "description": field.get("description")}
            for fieild_key, field in value.items()
        ]


class DatasetRow(BaseModel):
    attributes: dict[str, str]
    datacube: Kube


class ProductMetadata(BaseModel):
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
    _SUPPORTED_FORMATS_LABELS: ClassVar[dict[str, str]] = {
        "grib": "GRIB",
        "pickle": "PICKLE",
        "netcdf": "netCDF",
        "geotiff": "geoTIFF",
    }
    id: str
    data: list[DatasetRow]
    metadata: ProductMetadata
    description: Optional[str] = None
    dataset: DatasetMetadata

    @validator("description", always=True)
    def match_description(cls, value, values):
        if value is None:
            return values["id"]
        return value


class WidgetsCollection(BaseModel):
    version: Optional[str] = "v1"
    status: Optional[str] = "OK"
    id: str
    label: str
    dataset: DatasetMetadata
    widgets: list[dict]
    widgets_order: list[str]
