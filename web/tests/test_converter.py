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
        breakpoint()
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
        breakpoint()
        info = {}
        entry = open_catalog("../resources/catalogs/external/e-obs.yaml")
        if entry.metadata:
            info["metadata"] = entry.metadata
        info["products"] = {}
        for product_id in entry:
            info["products"][product_id] = eobs_ensemble_info
        return info

    def test_proper_template_loading_defaults(self):
        assert Converter.PRODUCT_TEMPLATE is None
        assert Converter.LIST_DATASET_TEMPLATE is None
        Converter.load_templates()
        assert Converter.PRODUCT_TEMPLATE is not None
        assert Converter.LIST_DATASET_TEMPLATE is not None

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
