import os
import logging

from jinja2 import Environment, FileSystemLoader, Template
from jinja2 import exceptions as ex


class Converter:

    _LOG = logging.getLogger("Converter")
    RESURCE_DIR = "./resources"
    DEFAULT_TEMPLATE_FILE = "basic_product_template.json"
    TEMPLATE = None

    @classmethod
    def load_template(cls, template_file: str = None):
        if not template_file:
            template_file = cls.DEFAULT_TEMPLATE_FILE
        loader = FileSystemLoader(searchpath=cls.RESURCE_DIR)
        try:
            cls.TEMPLATE = Environment(loader=loader).get_template(
                template_file
            )
        except ex.TemplateNotFound as e:
            cls._LOG.error(
                f"Template `{os.path.join(cls.RESURCE_DIR, template_file)}`"
                " was not found"
            )
            raise e
