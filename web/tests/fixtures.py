import pytest

from app.converter import Converter


@pytest.fixture(autouse=True)
def clear_converter():
    Converter.TEMPLATE = None
