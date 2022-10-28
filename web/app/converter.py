"""Module containing utils classes for view data for the Webportal"""
import os
import json
import logging

from jinja2 import Environment, FileSystemLoader
from jinja2.environment import Template
from jinja2 import exceptions as ex


class Converter:
    """Class managing rendering datasets and details based on Jinja2
    templates"""

    _LOG = logging.getLogger("Converter")
    RESOURCE_DIR = os.path.join(".", "templates")
    DEFAULT_LIST_DETAILS_TEMPLATE_FILE = "basic_list_datasets.json.jinja2"
    DEFAULT_PRODUCT_TEMPLATE_FILE = "basic_product.json.jinja2"
    ENVIRONMENT = None
    PRODUCT_TEMPLATE: Template = None
    LIST_DATASET_TEMPLATE: Template = None

    @classmethod
    def load_templates(
        cls,
        list_dataset_template_file: str = None,
        product_template_file: str = None,
    ):
        """Load Jinja2 templates for rendering datasets list
        (`list_dataset_template_file`) and product details JSON files
        (`product_template_file`). If not provided, default templates will
        be loaded.

        Parameters
        ----------
        list_dataset_template_file : str, optional, default=None
            Path to the Jinja2 template for rendering listing dataset JSON.
            It must be located under `template` directory.
            If `None`, default template will be used.
        product_template_file : str, optional, default=None
            Path to the Jinja2 template for rendering dataset details JSON
            It must be located under `template` directory.
            If `None`, default template will be used.

        Raises
        -------
        TemplateNotFound
            If one of templates was not found
        """
        cls._LOG.info("Loading Jinja2 template...")
        if product_template_file is None:
            product_template_file = cls.DEFAULT_PRODUCT_TEMPLATE_FILE
        if list_dataset_template_file is None:
            list_dataset_template_file = cls.DEFAULT_LIST_DETAILS_TEMPLATE_FILE
        loader = FileSystemLoader(searchpath=cls.RESOURCE_DIR)
        cls.ENVIRONMENT = Environment(loader=loader)
        try:
            cls.PRODUCT_TEMPLATE = cls.ENVIRONMENT.get_template(
                product_template_file
            )
            cls.LIST_DATASET_TEMPLATE = cls.ENVIRONMENT.get_template(
                list_dataset_template_file
            )
        except ex.TemplateNotFound as exception:
            cls._LOG.error(
                "one of templates `%s` or `%s` was not found",
                os.path.join(cls.RESOURCE_DIR, product_template_file),
                os.path.join(cls.RESOURCE_DIR, list_dataset_template_file),
            )
            raise exception

    @classmethod
    def render_list_datasets(cls, details: list) -> str:
        """Render datasets list JSON file based on associated Jinja2
        template and provided `details` object.

        Parameters
        ----------
        details : list
            List of dictionaries containing datasets details

        Returns
        -------
        json_details
            JSON text with list rendered accoridng to Jinja2 template
        """
        cls._LOG.debug("rendering list of datasets")
        return cls.LIST_DATASET_TEMPLATE.render(data=details)

    @classmethod
    def render_details(cls, details: dict) -> str:
        """Render dataset details JSON file based on associated Jinja2
        template and provided `details` object.

        Parameters
        ----------
        details : dict
            Dict representing details of the product

        Returns
        -------
        json_details
            JSON text with details rendered accoridng to Jinja2 template
        """
        cls._LOG.debug("rendering details for")
        # TODO: add method for widget-dict creation
        # args = cls.construct_dict(details)
        return json.loads(cls.PRODUCT_TEMPLATE.render(dataset=details))


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
