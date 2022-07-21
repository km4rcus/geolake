import pytest

from jinja2 import exceptions as ex

from app.converter import Converter


class TestConverter:
    @pytest.fixture(autouse=True)
    def clear_converter(self):
        Converter.TEMPLATE = None

    def test_proper_template_loading(self):
        assert Converter.TEMPLATE is None
        Converter.load_template("basic_product_template.json")
        assert Converter.TEMPLATE is not None

    def test_error_on_missing_template(self):
        assert Converter.TEMPLATE is None
        with pytest.raises(
            ex.TemplateNotFound, match=r"missing-template.json"
        ):
            Converter.load_template("missing-template.json")
