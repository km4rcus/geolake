"""Module containing utils classes for view data for the Webportal"""
import os
import logging

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
        return details
        widgets, widgets_order = cls._convert_products_details_to_widgets(
            details["products"]
        )
        return cls.PRODUCT_TEMPLATE.render(
            dataset=details, widgets=widgets, widgets_order=widgets_order
        )

    @classmethod
    @log_execution_time(_LOG)
    def _convert_products_details_to_widgets(
        cls, products_details: dict
    ) -> list:
        all_prods_details = {}
        for prod_id, prod_det in products_details.items():
            all_prods_details[
                prod_id
            ] = cls._convert_single_product_details_to_widgets(prod_det)

        return all_prods_details

    @classmethod
    def _convert_single_product_details_to_widgets(cls, prod_details: dict):
        data = {"widgets": [], "widgets_order": [], "constraints": None}
        attrs_widgets, attrs_widgets_order = Converter.get_attrs_widgets(
            prod_details
        )
        data["widgets"].extend(attrs_widgets)
        data["widgets_order"].extend(attrs_widgets_order)

        field_widget = Converter.get_field_widget(prod_details)
        data["widgets"].append(field_widget)
        data["widgets_order"].append("variable")

        temporal_widgets = ...
        data["widgets"].extend(temporal_widgets)
        data["widgets_order"].append("temporal_coverage")

        spatial_widgets, spatial_widgets_order = ...
        data["widgets"].extend(spatial_widgets)
        data["widgets_order"].append(spatial_widgets_order)

    @staticmethod
    def get_attrs_widgets(prod_details: dict):
        attr_labels = {
            att_det["name"]: att_det.get("label", att_det["name"])
            for att_det in prod_details.get("filters", [])
        }
        attrs_opts = defaultdict(list)
        for kube_det in prod_details["details"]:
            for att_id, att_val in kube_det["attributes"].items():
                attrs_opts[att_id].append(att_val)
        for key in attrs_opts.keys():
            attrs_opts[key] = sorted(attrs_opts[key])
        widgets = []
        for att_key, att_opts in attrs_opts.items():
            w = Widget(
                wname=att_key,
                wlabel=attr_labels[att_key],
                wrequired=False,
                wparameter=att_key,
                wtype="StringList",
                wdetails={"values": att_opts},
            )
            widgets.append(w)
        widgets_order = list(attr_labels.keys())
        return (widgets, widgets_order)

    @staticmethod
    def get_field_widgets(prod_details: dict):
        return None
        # kube_det["datacube"]["fields"].keys() for kube_det in prod_details["details"]

    @staticmethod
    def _get_all_fields(prod_details: list):
        for kube_det in prod_details:
            kube_det["fields"]
        details = prod_details["details"]

    @staticmethod
    def _get_attribute_options(details: list[dict], sort_values=False):

        return attrs_opts

    @staticmethod
    def _get_widget_for_fields(details: list[dict], sort_values=False):
        pass

    @staticmethod
    def _get_widget_for_attrs(attrs: dict):
        pass
