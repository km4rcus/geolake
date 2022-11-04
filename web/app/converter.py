"""Module containing utils classes for view data for the Webportal"""
import os
import logging
from typing import ClassVar, Optional
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

    @validator("default", always=True)
    def take_default(cls, value, values):
        if value is None:
            return next(iter(values["products"].keys()))
        return value

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
