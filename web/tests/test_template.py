import os
import pytest
import json
import yaml

from jinja2 import Environment, FileSystemLoader, Template

import app.jinja_filter as jf


class TestTemplate:
    @pytest.fixture
    def list_datasets_template_name(self):
        yield "basic_list_datasets.json.jinja"

    @pytest.fixture
    def product_template_name(self):
        yield "basic_product.json.jinja"

    @pytest.fixture
    def filters_dict(self):
        return {"required": jf.required, "escape_chars": jf.escape_chars}

    @pytest.fixture
    def root_metadata(self):
        yield {
            "version": "v.1.0",
            "status": "OK",
        }

    @pytest.fixture
    def list_dataset_template(self, list_datasets_template_name, filters_dict):
        env = Environment(loader=FileSystemLoader(searchpath="resources"))
        env.filters.update(filters_dict)
        yield env.get_template(list_datasets_template_name)

    @pytest.fixture
    def product_template(self, product_template_name, filters_dict):
        env = Environment(loader=FileSystemLoader(searchpath="resources"))
        env.filters.update(filters_dict)
        yield env.get_template(product_template_name)

    @pytest.fixture
    def dataset_metadata(self):
        with open(
            os.path.join("tests", "resources", "dataset_metadata.yaml"),
            "rt",
            encoding="utf-8",
        ) as f:
            yield yaml.safe_load(f)

    @pytest.mark.parametrize("drop_key", ["version", "status"])
    def test_fail_render_if_missing_required_key(
        self, list_dataset_template, root_metadata, drop_key
    ):
        _ = root_metadata.pop(drop_key, None)
        with pytest.raises(KeyError, match=f"Key `{drop_key}`*"):
            _ = list_dataset_template.render(root_metadata)

    def test_render_list_datasets(
        self, root_metadata, list_dataset_template, dataset_metadata
    ):
        args_dict = dict(
            **root_metadata,
            data=[
                dict(id="era5", **dataset_metadata),
                dict(id="e-obs", **dataset_metadata),
            ],
        )
        res = list_dataset_template.render(args_dict)
        details = json.loads(res)
        assert details.keys() == {"status", "version", "data"}
        assert len(details["data"]) == 2
        assert details["data"][0]["id"] == "era5"
        assert details["data"][1]["id"] == "e-obs"
