import pytest
import json

from jinja2 import Environment, FileSystemLoader, Template


class TestTemplate:
    @pytest.fixture
    def template_name(self):
        yield "basic_product_template.json"

    @pytest.fixture
    def template(self, template_name):
        yield Environment(
            loader=FileSystemLoader(searchpath="./resources")
        ).get_template(template_name)

    def test_render_only_metadata_if_other_not_passed(self, template):
        args = {
            "version": "v.1.0",
            "status": "OK",
            "dataset_label": "ERA5 Single levels",
        }
        json_res = template.render(args)
        parsed_dict = json.loads(json_res)
        assert parsed_dict.keys() == {"version", "status", "label"}
