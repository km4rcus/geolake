"""Module with tools for widgets management"""
import logging
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta
from numbers import Number

import numpy as np
import pandas as pd
from pandas.tseries.frequencies import to_offset
from pydantic import validate_arguments

from .meta import LoggableMeta
from .utils import log_execution_time, maybe_round_value
from .models import Filter, Product, WidgetsCollection, Kube


def min_max_dict(min_val=np.inf, max_val=-np.inf):
    """Create a default dictionary with 'min' and 'max' keys"""
    return {"min": min_val, "max": max_val}


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


class WidgetFactory(metaclass=LoggableMeta):
    """Class which prepares widgets for Web portal"""

    _LOG = logging.getLogger("Widget")
    _MAIN_COORDS = {"time", "latitude", "longitude"}
    _NUMBER_OF_DECIMALS = 2
    _FREQ_CODES = {
        "T": "minute",
        "H": "hour",
        "D": "day",
        "M": "month",
        "Y": "year",
    }

    @log_execution_time(_LOG)
    @validate_arguments
    def __init__(self, product: Product):
        self._LOG.debug(f"provided filters: %s", product.metadata.filters)
        self._d = product
        self._wid = []
        self._wid_order = []
        self._compute_variable_widget()
        self._compute_attrs_widgets()
        self._compute_temporal_widgets()
        self._compute_spatial_widgets()
        self._compute_auxiliary_coords_widgets()
        self._compute_format_widget()

    def _is_for_skipping(self, name):
        self._LOG.debug("checking if '%s' should be skipped", name)
        if (flt := self._d.metadata.filters.get(name)) is not None:
            self._LOG.debug(
                "should '%s' be skipped - %s ", name, not flt.user_defined
            )
            return not flt.user_defined
        self._LOG.debug("filter for '%s' was not found. retaining", name)
        return False

    def _maybe_get_label(self, name, default=None):
        self._LOG.debug("checking label for '%s'", name)
        if (flt := self._d.metadata.filters.get(name)) is not None:
            self._LOG.debug("found label for '%s' - ", not flt.label)
            return flt.label
        self._LOG.debug("filter for '%s' was not found", name)
        return default if default is not None else name

    @property
    @log_execution_time(_LOG)
    def widgets(self) -> dict:
        """Get the entire collection of Web portal widgets in the predefined
        order:
        1. variables widget
        2. attributes widgets
        3. temporal selection widgets
        4. spatial selection widgets
        5. format widgets

        Returns
        -------
        widgets : WidgetsCollection
            The collection of widgets for Web portal
        """
        return WidgetsCollection(
            id=self._d.id,
            label=self._d.description,
            dataset=self._d.dataset,
            widgets=self._wid,
            widgets_order=self._wid_order,
        )

    def _compute_variable_widget(self, sort_keys: bool = True) -> None:
        all_fields = {}
        for dr in self._d.data:
            if dr is None:
                # NOTE: it means, datacube is Delayed object
                # we don't care about variables
                continue
            if isinstance(dr, Kube):
                fields = dr.fields
            else:
                if dr.datacube is None:
                    # NOTE: it means, datacube is Delayed object
                    # we don't care about variables
                    continue
                fields = dr.datacube.fields
            for field in fields:
                if field.name in all_fields:
                    continue
                if self._is_for_skipping(field.name):
                    continue
                all_fields[field.name] = {
                    "value": field.name,
                    "label": self._maybe_get_label(
                        field.name, field.description
                    ),
                }
        if not all_fields:
            return
        if sort_keys:
            all_fields = OrderedDict(all_fields)
        self._wid.append(
            Widget(
                wname="variable",
                wlabel="Variables",
                wrequired=True,
                wparameter="variable",
                wtype="StringList",
                wdetails={"values": list(all_fields.values())},
            ).to_dict()
        )
        self._wid_order.append("variable")

    def _compute_attrs_widgets(self, sort_keys: bool = True) -> None:
        attrs_opts = defaultdict(set)
        for dr in self._d.data:
            if isinstance(dr, Kube):
                # NOTE: there is only one Kube in this case,
                # so we can exit function
                return
            for att_name, att_val in dr.attributes.items():
                attrs_opts[att_name].add(att_val)
        for att_name, att_opts in attrs_opts.items():
            if self._is_for_skipping(att_name):
                continue
            if (flt := self._d.metadata.filters.get(att_name)) is not None:
                label = flt.label
            else:
                label = att_name
            att_opts = list(att_opts)
            if sort_keys:
                att_opts = sorted(att_opts)
            values = [{"value": key, "label": key} for key in att_opts]
            wid = Widget(
                wname=att_name,
                wlabel=label,
                wrequired=False,
                wparameter=att_name,
                wtype="StringChoice",
                wdetails={"values": values},
            )
            self._wid.append(wid.to_dict())
            self._wid_order.append(att_name)

    def _compute_temporal_widgets(self) -> None:
        temporal_coords = min_max_dict(min_val=None, max_val=None)
        min_time_step = np.inf
        time_unit = None
        for dr in self._d.data:
            if dr is None:
                # NOTE: it means, datacube is Delayed object
                # we don't care about coordinates
                continue
            if isinstance(dr, Kube):
                coords = dr.domain.coordinates
            else:
                if dr.datacube is None:
                    # NOTE: it means, datacube is Delayed object
                    # we don't care about coordinates
                    continue
                coords = dr.datacube.domain.coordinates
            if "time" not in coords:
                continue
            time_vals = np.array(coords["time"].values, dtype=np.datetime64)
            if len(time_vals) < 2:
                continue
            time_offset = to_offset(pd.Series(time_vals).diff().mode().item())
            current_time_unit = time_offset.name
            current_time_step = time_offset.n
            if current_time_step < min_time_step:
                min_time_step = current_time_step
                time_unit = current_time_unit

            if temporal_coords["max"]:
                temporal_coords["max"] = max(
                    [temporal_coords["max"], max(time_vals)]
                )
            else:
                temporal_coords["max"] = max(time_vals)
            if temporal_coords["min"]:
                temporal_coords["min"] = min(
                    [temporal_coords["min"], min(time_vals)]
                )
            else:
                temporal_coords["min"] = min(time_vals)

        if not (temporal_coords["min"] and temporal_coords["max"]):
            return
        time_unit = self._FREQ_CODES[time_unit]
        temporal_coords["max"] = (
            temporal_coords["max"].astype("M8[h]").astype("O")
        )
        temporal_coords["min"] = (
            temporal_coords["min"].astype("M8[h]").astype("O")
        )
        wid = Widget(
            wname="temporal_coverage",
            wlabel="Temporal coverage",
            wrequired=True,
            wparameter=None,
            wtype="ExclusiveFrame",
            wdetails={"widgets": ["date_list", "date_range"]},
        )
        self._wid.append(wid.to_dict())
        step = (
            timedelta(**{f"{time_unit}s": min_time_step})
            if time_unit in {"day", "hour"}
            else timedelta(
                days=min_time_step * (365 if time_unit == "year" else 30)
            )
        )
        time_widgets = {
            "year": [
                {"label": str(y), "value": str(y)}
                for y in range(
                    temporal_coords["min"].year,
                    temporal_coords["max"].year + 1,
                )
            ]
        }
        if (time_unit == "month" and min_time_step < 12) or step < timedelta(
            days=365
        ):
            months = (
                range(
                    temporal_coords["min"].month,
                    temporal_coords["max"].month + 1,
                )
                if len(time_widgets["year"]) == 1
                else range(1, 13)
            )
            time_widgets["month"] = [
                {
                    "label": datetime.strptime(str(m), "%m").strftime("%B"),
                    "value": str(m),
                }
                for m in months
            ]
            if step < timedelta(days=28):
                time_widgets["day"] = [
                    {"label": str(d), "value": str(d)} for d in range(1, 32)
                ]
                if step < timedelta(hours=24):
                    minute = f"{temporal_coords['min'].minute:02d}"
                    time_widgets["hour"] = [
                        {"label": f"{h:02}", "value": f"{h:02}"}
                        for h in range(24)
                    ]
        wid = Widget(
            wname="date_list",
            wlabel="Date",
            wrequired=True,
            wparameter=None,
            wtype="InclusiveFrame",
            wdetails={"widgets": list(time_widgets.keys())},
        )
        self._wid.append(wid.to_dict())

        for freq, time_values in time_widgets.items():
            wid = Widget(
                wname=freq,
                wlabel=freq.capitalize(),
                wrequired=True,
                wparameter=f"time:{freq}",
                wtype="StringList",
                wdetails={"values": time_values},
            )
            self._wid.append(wid.to_dict())

        t_range = [
            {
                "name": "start",
                "label": "Start Date",
                "range": temporal_coords["min"],
            },
            {
                "name": "stop",
                "label": "End Date",
                "range": temporal_coords["max"],
            },
        ]
        wid = Widget(
            wname="date_range",
            wlabel="Date range",
            wrequired=True,
            wparameter="time",
            wtype="DateTimeRange",
            wdetails={"fields": t_range},
        )

        self._wid.append(wid.to_dict())
        self._wid_order.append("temporal_coverage")

    def _compute_spatial_widgets(self) -> None:
        spatial_coords = defaultdict(min_max_dict)
        for dr in self._d.data:
            if dr is None:
                # NOTE: it means, datacube is Delayed object
                # we don't care about coordinates
                continue
            if isinstance(dr, Kube):
                coords = dr.domain.coordinates
            else:
                if dr.datacube is None:
                    # NOTE: it means, datacube is Delayed object
                    # we don't care about coordinates
                    continue
                coords = dr.datacube.domain.coordinates
            if "latitude" not in coords or "longitude" not in coords:
                continue
            for coord_name in ["latitude", "longitude"]:
                values = np.array(coords[coord_name].values).astype(np.float)
                spatial_coords[coord_name]["max"] = max(
                    [spatial_coords[coord_name]["max"], np.max(values)]
                )
                spatial_coords[coord_name]["min"] = min(
                    [spatial_coords[coord_name]["min"], np.min(values)]
                )
        if not spatial_coords:
            return

        wid = Widget(
            wname="spatial_coverage",
            wlabel="Spatial coverage",
            wrequired=False,
            wparameter=None,
            wtype="ExclusiveFrame",
            wdetails={"widgets": ["area", "location"]},
        )
        self._wid.append(wid.to_dict())
        self._wid_order.append("spatial_coverage")

        area_fields = [
            {
                "name": orient,
                "label": orient.capitalize(),
                "range": round(
                    spatial_coords[coord][ext], self._NUMBER_OF_DECIMALS
                ),
            }
            for orient, coord, ext in zip(
                ("north", "west", "south", "east"),
                ("latitude", "longitude") * 2,
                ("max", "min", "min", "max"),
            )
        ]
        wid = Widget(
            wname="area",
            wlabel="Area",
            wrequired=True,
            wparameter="area",
            wtype="geoarea",
            wdetails={"fields": area_fields},
        )
        self._wid.append(wid.to_dict())

        loc_fields = [
            {
                "name": coord,
                "label": coord.capitalize(),
                "range": [
                    round(spatial_coords[coord][ext], self._NUMBER_OF_DECIMALS)
                    for ext in ("min", "max")
                ],
            }
            for coord in ("latitude", "longitude")
        ]
        wid = Widget(
            wname="location",
            wlabel="Location",
            wrequired=True,
            wparameter="location",
            wtype="geolocation",
            wdetails={"fields": loc_fields},
        )
        self._wid.append(wid.to_dict())

    def _get_aux_coord_names(self, all_coords_names):
        return list(set(all_coords_names) - self._MAIN_COORDS)

    def _compute_auxiliary_coords_widgets(self) -> None:
        aux_coords = defaultdict(dict)
        for dr in self._d.data:
            if dr is None:
                # NOTE: it means, datacube is Delayed object
                # we don't care about coordinates
                continue
            if isinstance(dr, Kube):
                coords = dr.domain.coordinates
            else:
                if dr.datacube is None:
                    # NOTE: it means, datacube is Delayed object
                    # we don't care about coordinates
                    continue
                coords = dr.datacube.domain.coordinates
            if (
                len(
                    aux_kube_coords_names := self._get_aux_coord_names(
                        coords.keys()
                    )
                )
                == 0
            ):
                continue
            for coord_name in aux_kube_coords_names:
                if self._is_for_skipping(coord_name):
                    continue

                # TODO: `vals` might be 2d. what to do? compute uniqe?
                vals = np.unique(np.array(coords[coord_name].values))
                try:
                    vals = vals.astype(np.float)
                except ValueError:
                    self._LOG.info(
                        "skipping coordinate '%s' - non-castable to float"
                        " (%s)",
                        coord_name,
                        vals,
                    )
                    continue
                # elif (cast_vals := WidgetFactory._maybe_cast_to_datetime64(vals)) is not None:
                #     vals = cast_vals

                if "min" in aux_coords[coord_name]:
                    aux_coords[coord_name]["min"] = min(
                        [
                            aux_coords[coord_name]["min"],
                            min(vals),
                        ]
                    )
                else:
                    aux_coords[coord_name]["min"] = min(vals)
                if "max" in aux_coords[coord_name]:
                    aux_coords[coord_name]["max"] = max(
                        [
                            aux_coords[coord_name]["max"],
                            max(vals),
                        ]
                    )
                else:
                    aux_coords[coord_name]["max"] = max(vals)

                aux_coords[coord_name]["values"] = sorted(vals)
                aux_coords[coord_name]["label"] = self._maybe_get_label(
                    coord_name, coords[coord_name].label
                )
                aux_coords[coord_name]["name"] = coords[coord_name].name
        if not aux_coords:
            return
        for coord_name, coord_value in aux_coords.items():
            wid = Widget(
                wname=coord_name,
                wlabel=coord_name.capitalize(),
                wrequired=True,
                wparameter=None,
                wtype="ExclusiveFrame",
                wdetails={
                    "widgets": [f"{coord_name}_list", f"{coord_name}_range"]
                },
            )
            self._wid.append(wid.to_dict())
            self._wid_order.append(coord_name)

            values = [
                {
                    "value": val,
                    "label": "{:.2f}".format(
                        maybe_round_value(val, self._NUMBER_OF_DECIMALS)
                    ),
                }
                for val in coord_value["values"]
            ]

            wid = Widget(
                wname=f"{coord_name}_list",
                wlabel=coord_name.capitalize(),
                wrequired=False,
                wparameter=coord_name,
                wtype="StringList",
                wdetails={"values": values},
            )
            self._wid.append(wid.to_dict())
            if "max" in coord_value and "min" in coord_value:
                range_ = [
                    {
                        "name": "start",
                        "label": f"Min {coord_name}",
                        "range": coord_value["min"],
                    },
                    {
                        "name": "stop",
                        "label": f"Max {coord_name}",
                        "range": coord_value["max"],
                    },
                ]
                wid = Widget(
                    wname=f"{coord_name}_range",
                    wlabel=coord_name.capitalize(),
                    wrequired=False,
                    wparameter=coord_name,
                    wtype="NumberRange",
                    wdetails={"fields": range_},
                )
                self._wid.append(wid.to_dict())

    def _compute_format_widget(self) -> None:
        wid = Widget(
            wname="format",
            wlabel="Format",
            wrequired=True,
            wparameter="format",
            wtype="FileFormat",
            wdetails={
                "values": [
                    {"value": "netcdf", "label": "netCDF", "ext": ".nc"}
                ]
            },  # TODO: more formats
        )
        self._wid.append(wid.to_dict())
        self._wid_order.append("format")
