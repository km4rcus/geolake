"""Module with tools for widgets management"""
import logging
from collections import defaultdict

from typing import Any, ClassVar, Optional

from pydantic import BaseModel, root_validator, validator, validate_arguments

from .meta import LoggableMeta
from .util import log_execution_time
from .converter import DatasetMetadata


class Widget:
    """Class representing a single Widget in the Webportal"""

    def __init__(
        self,
        wname,
        wlabel,
        wrequired,
        wparameter,
        wtype,
        wdetails=None,
        whelp=None,
        winfo=None,
    ):
        self.__data = {
            "name": str(wname),
            "label": str(wlabel),
            "required": bool(wrequired),
            "parameter": str(wparameter) if wparameter is not None else None,
            "type": str(wtype),
            "details": wdetails,
            "help": whelp,
            "info": winfo,
        }

    def __getitem__(self, key):
        return self.__data[key]

    def to_dict(self):
        """Return dictionary representation of a Widget

        Returns
        -------
        widget_dict
            Dictionary with keys being attributes of a Widget object
        """
        return self.__data.copy()

    @classmethod
    def from_dict(cls, data):
        """Construct Widget object based on the provided dictionary

        Parameters
        ----------
        data : dict
            Dict representing attributes of a Widget

        Returns
        -------
        widget
            Widget object
        """
        return Widget(**data)


class _Filter(BaseModel):
    name: Optional[str] = None
    user_defined: Optional[bool] = True
    label: Optional[str] = None

    @root_validator
    def match_label(cls, values):
        if values["label"] is None:
            values["label"] = values["name"]
        return values

    @validator("user_defined", pre=True)
    def maybe_cast_user_defined(cls, value):
        if isinstance(value, str):
            return value.lower() in ["t", "true"]
        elif isinstance(value, bool):
            return value
        else:
            raise TypeError


class _Domain(BaseModel):
    crs: dict[str, Any]
    coordinates: dict[str, Any]


class _Kube(BaseModel):
    domain: _Domain
    fields: list[str]

    @validator("fields", pre=True)
    def match_fields(cls, fields):
        return list(fields.keys())


class _DatasetRow(BaseModel):
    attributes: dict[str, str]
    datacube: _Kube


class ProductDatasetMetadata(BaseModel):
    catalog_dir: str
    filters: Optional[dict[str, _Filter]] = None
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


class _Product(BaseModel):
    _SUPPORTED_FORMATS_LABELS: ClassVar[dict[str, str]] = {
        "grib": "GRIB",
        "pickle": "PICKLE",
        "netcdf": "netCDF",
        "geotiff": "geoTIFF",
    }
    id: str
    data: list[_DatasetRow]
    metadata: ProductDatasetMetadata
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


class WidgetFactory(metaclass=LoggableMeta):
    _LOG = logging.getLogger("Widget")

    @log_execution_time(_LOG)
    @validate_arguments
    def __init__(self, product: _Product):
        self._d = product
        self._wid = []
        self._wid_order = []
        self.compute_variable_widget()
        self.compute_attrs_widgets()
        self.compute_temporal_widgets()
        self.compute_spatial_widgets()
        self.compute_format_widget()

    @property
    @log_execution_time(_LOG)
    def widgets(self) -> dict:
        return WidgetsCollection(
            id=self._d.id,
            label=self._d.description,
            dataset=self._d.dataset,
            widgets=self._wid,
            widgets_order=self._wid_order,
        )

    def get_widgets_for(self, key: str):
        return self._wid[key]

    def get_widgets_order_for(self, key: str):
        return self._wid_order[key]

    def compute_variable_widget(self, sort_keys: bool = True) -> None:
        all_fields = set()
        for dr in self._d.data:
            all_fields.update(dr.datacube.fields)
        values = [{"value": ..., "label": ...}]
        self._wid.append(
            Widget(
                wname="variable",
                wlabel="Variables",
                wrequired=True,
                wparameter="variable",
                wtype="StringList",
                wdetails={
                    "values": sorted(list(all_fields))
                    if sort_keys
                    else list(all_fields)
                },
            ).to_dict()
        )
        self._wid_order.append("variable")

    def compute_attrs_widgets(self, sort_keys: bool = True) -> None:
        attrs_opts = defaultdict(set)
        for dr in self._d.data:
            for att_name, att_val in dr.attributes.items():
                attrs_opts[att_name].add(att_val)
        for att_name, att_opts in attrs_opts.items():
            flt = self._d.metadata.filters.get(att_name, _Filter)
            if not flt.user_defined:
                # then it is skipped and user cannot manipulate it
                continue
            label = flt.label if flt.label is not None else att_name
            wid = Widget(
                wname=att_name,
                wlabel=label,
                wrequired=False,
                wparameter=att_name,
                wtype="StringList",
                wdetails={
                    "values": sorted(list(att_opts))
                    if sort_keys
                    else list(att_opts)
                },
            )
            self._wid.append(wid.to_dict())
            self._wid_order.append(att_name)

    def compute_temporal_widgets(self) -> None:
        pass

    def compute_spatial_widgets(self) -> None:
        pass

    def compute_format_widget(self) -> None:
        w = Widget(
            wname="format",
            wlabel="Format",
            wrequired=True,
            wparameter="format",
            wtype="FileFormat",
            wdetails={
                "values": [{"value": "netcdf", "label": "netCDF"}]
            },  # TODO:
        )
        self._wid.append(w.to_dict())
        self._wid_order.append("format")
