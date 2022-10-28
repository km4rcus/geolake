import datetime
import os
import pytest
from intake import open_catalog

from jinja2 import exceptions as ex

from app.converter import Converter


class TestConverter:
    @pytest.fixture(autouse=True)
    def clear_converter(self):
        Converter.TEMPLATE = None
        Converter.ENVIRONMENT = None

    @pytest.fixture
    def eobs_ensemble_info(self):
        info = {}
        entry = open_catalog("../resources/catalogs/external/e-obs.yaml")[
            "ensemble"
        ]
        if entry.metadata:
            info["metadata"] = entry.metadata
        info["data"] = entry.read_chunked().to_dict()
        return info

    @pytest.fixture
    def eobs_details(self, eobs_ensemble_info):
        info = {}
        entry = open_catalog("../resources/catalogs/external/e-obs.yaml")
        if entry.metadata:
            info["metadata"] = entry.metadata
        info["products"] = {}
        for product_id in entry:
            info["products"][product_id] = eobs_ensemble_info
        return info

    def test_error_on_missing_template(self):
        assert Converter.TEMPLATE is None
        with pytest.raises(
            ex.TemplateNotFound, match=r"missing-template.json"
        ):
            Converter.load_templates("missing-template.json")

    def test_render_details_eobs(self, eobs_details):
        import json

        details = Converter.render_details(eobs_details)
        details_dict = json.loads(details)
        assert "version" in details
        assert details["version"] == 1.0
        assert len(details_dict["products"]) == 1
