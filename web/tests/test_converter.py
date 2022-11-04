import datetime
import os
import json
import pytest

from app.converter import ListOfDatasets


class TestConverter:
    @pytest.fixture
    def resource_dir(self):
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "resources"
        )

    @pytest.fixture
    def details(self, resource_dir):
        with open(
            os.path.join(resource_dir, "sample_details.json"), "rt"
        ) as file:
            yield json.load(file)

    def test_parse_details_successfully(self, details):
        lod = ListOfDatasets.from_details(details)
        assert isinstance(lod, ListOfDatasets)
