import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.main import app
from app.util import UserCredentials
from app.components.access import AccessManager


@pytest.fixture(scope="session")
def client():
    client = TestClient(app)
    yield client


@pytest.fixture(scope="function")
def access_manager():
    with patch("app.components.dataset.AccessManager") as p:
        p.authenticate_user = MagicMock(return_value=True)
        p.is_user_eligible_for_role = MagicMock(
            side_effect=is_user_elg_for_role
        )
        yield p


@pytest.fixture(scope="function")
def data_store():
    with patch("app.components.dataset.Datastore") as p:
        inst = MagicMock()
        p.return_value = inst
        inst.dataset_list = MagicMock(return_value=["e-obs", "era5"])
        inst.product_list = MagicMock(side_effect=_get_prods_for_ds)
        inst.product_metadata = MagicMock(side_effect=_get_prods_meta)
        yield inst


@pytest.fixture(scope="session")
def user_credentials():
    with patch("app.components.dataset.UserCredentials") as p:
        p.return_value = UserCredentials("1:1234")
        yield p


def _get_prods_for_ds(dataset_id):
    if dataset_id == "e-obs":
        return ["ensemble", "spread"]
    elif dataset_id == "era5":
        return ["reanalysis"]


def _get_prods_meta(dataset_id, product_id):
    role = "public"
    if dataset_id == "e-obs":
        if product_id == "ensemble":
            role = "internal"
        elif product_id == "spread":
            role = "admin"
    return {"role": role}


def is_user_elg_for_role(user_credentials, product_role_name):
    if product_role_name == "public":
        return True
    if user_credentials.is_public:
        return False
    user_role_name = "internal"
    if user_role_name == "admin":
        return True
    elif user_role_name == product_role_name:
        return True
    else:
        return False
