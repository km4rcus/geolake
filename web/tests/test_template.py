import os
import pytest
import json
import yaml

from jinja2 import Environment, FileSystemLoader, Template

import app.jinja_filter as jf


class TestTemplate:
    @pytest.fixture
    def template_name(self):
        yield "basic_product_template.json.jinja"

    @pytest.fixture
    def filters_dict(self):
        return {"required": jf.required, "escape_chars": jf.escape_chars}

    @pytest.fixture
    def template(self, template_name, filters_dict):
        env = Environment(loader=FileSystemLoader(searchpath="resources"))
        env.filters.update(filters_dict)
        yield env.get_template(template_name)

    @pytest.fixture
    def dataset_metadata(self):
        with open(
            os.path.join("tests", "resources", "dataset_metadata.yaml"), "rt"
        ) as f:
            yield yaml.safe_load(f)

    @pytest.fixture
    def details_meta(self):
        yield {
            "version": "v.1.0",
            "status": "OK",
            "dataset_label": "ERA5 Single levels",
            "id": "reanalysis",
        }

    @pytest.mark.parametrize(
        "drop_key", ["version", "status", "dataset_label", "id"]
    )
    def test_fail_render_if_missing_required_key(
        self, template, details_meta, drop_key
    ):
        _ = details_meta.pop(drop_key, None)
        with pytest.raises(KeyError, match=f"Key `{drop_key}`*"):
            _ = template.render(details_meta)

    def test_render_only_metadata_if_other_not_passed(
        self, template, details_meta
    ):
        json_res = template.render(details_meta)
        parsed_dict = json.loads(json_res)
        assert parsed_dict.keys() == {
            "version",
            "status",
            "id",
            "label",
            "dataset",
            "widgets",
            "widgets_order",
        }
        assert parsed_dict["widgets"] == []
        assert parsed_dict["widgets_order"] == []

    def test_render_dataset_metadata(
        self, template, details_meta, dataset_metadata
    ):
        json_res = template.render(
            {**details_meta, "dataset": dataset_metadata}
        )
        parsed_dict = json.loads(json_res)
        assert parsed_dict["dataset"].pop("id") == ""
        assert parsed_dict["dataset"].pop("default") == ""
        for k in parsed_dict["dataset"].keys():
            if isinstance(parsed_dict["dataset"][k], (dict, list)):
                parsed_dict["dataset"][k] == dataset_metadata.get(k, {})
            else:
                assert parsed_dict["dataset"][k] == str(
                    dataset_metadata.get(k, "")
                )
