"""Module containing utils classes for view data for the Webportal"""
import os
import logging
from collections import defaultdict, OrderedDict

from jinja2 import Environment, FileSystemLoader
from jinja2.environment import Template
from jinja2 import exceptions as ex

from .util import log_execution_time
from .meta import LoggableMeta


class Converter(metaclass=LoggableMeta):
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
    @log_execution_time(_LOG)
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
    @log_execution_time(_LOG)
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
        cls._LOG.debug("rendering details")
        # TODO: add method for widget-dict creation
        # args = cls.construct_dict(details)
        widgets = cls._convert_products_details_to_widgets(details["products"])
        return cls.PRODUCT_TEMPLATE.render(
            dataset=details, widgets=[], widgets_order=[]
        )

    @classmethod
    @log_execution_time(_LOG)
    def _convert_products_details_to_widgets(
        cls, products_details: dict
    ) -> list:
        all_prods_details = {}
        for prod_id, prod_det in products_details.items():
            all_prods_details.update(
                cls._convert_single_product_details_to_widgets(prod_det["details"])
            )

        return all_prods_details

    @classmethod
    def _convert_single_product_details_to_widgets(cls, prod_details: list[dict]):
        attrs_opts = Converter._get_attributes_options(prod_details)        
        raise ValueError(prod_details)
        details = prod_details["details"]
        if isinstance(details, dict):
            # it means we have just DataCube
            pass
        elif isinstance(details, list):
            # it means we have a Dataset - so list[dict]
            pass
        else:
            raise TypeError(
                f"Unexpected type of 'details' value: '{type(details)}'."
                " Expected 'dict' or 'list'"
            )
        return {prod_details["id"]: [Converter._get_widget_for_fields(...)]}

    @staticmethod
    def _get_attribute_widget(details: list[dict], sort_keys=False, sort_values=False):
        attrs_opts = defaultdict(list)
        for kube_det in details:
            for att_id, att_val in kube_det["attributes"].items():
                attrs_opts[att_id].append(att_val)
        if sort_keys:
            attrs_opts = OrderedDict(attrs_opts)
        if sort_values:
            for key in attrs_opts.keys():
                attrs_opts[key] = sorted(attrs_opts[key])
        return attrs_opts

# w = Widget(wname=attr_name, wlabel=DatasetManager.unwrap(attr.get('label', {attr_name})), wrequired=False,
#                     wparameter=attr_name, wtype='StringList',
#                     wdetails={'values': values})        

    @staticmethod
    def _get_widget_for_fields(fields: dict):
        pass

    @staticmethod
    def _get_widget_for_attrs(attrs: dict):
        pass


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
