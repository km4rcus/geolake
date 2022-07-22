import os
import logging

from jinja2 import Environment, FileSystemLoader, Template
from jinja2 import exceptions as ex

import jinja_filter as jf


class Converter:

    _LOG = logging.getLogger("Converter")
    RESURCE_DIR = os.path.join(".", "resources")
    DEFAULT_TEMPLATE_FILE = "basic_product_template.json.jinja2"
    ENVIRONMENT = None
    TEMPLATE = None

    @classmethod
    def load_template(cls, template_file: str = None):
        cls._LOG("Loading Jinja2 template...")
        if not template_file:
            template_file = cls.DEFAULT_TEMPLATE_FILE
        loader = FileSystemLoader(searchpath=cls.RESURCE_DIR)
        cls.ENVIRONMENT = Environment(loader=loader)
        try:
            cls.TEMPLATE = cls.ENVIRONMENT.get_template(template_file)
        except ex.TemplateNotFound as e:
            cls._LOG.error(
                f"Template `{os.path.join(cls.RESURCE_DIR, template_file)}`"
                " was not found"
            )
            raise e
        cls.load_filters()

    @classmethod
    def load_filters(cls):
        cls._LOG("Loading custom filters for Jinja2 environment...")
        cls.ENVIRONMENT.filters["required"] = jf.required
        cls.ENVIRONMENT.filters["escape_chars"] = jf.escape_chars

    @classmethod
    def render_details(cls, details: dict) -> str:
        cls._LOG.debug(f"Rendering details for `{dataset_id}`...")
        args = cls.construct_dict(details)
        return cls.TEMPLATE.render(args)

    @classmethod
    def construct_dict(cls, details: dict):
        args = {}
        args["dataset"] = details.get("metadata", {})
        # TODO: construct widgets


class Widget:
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
        return self.__data.copy()

    @classmethod
    def from_dict(cls, data):
        return Widget(**data)
