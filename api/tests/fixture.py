import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.main import app
from app.utils.auth import UserCredentials


@pytest.fixture(scope="session")
def client():
    client = TestClient(app)
    yield client


@pytest.fixture(scope="function")
def access_manager():
    with patch("app.components.dataset.AccessManager") as p:
        p.authenticate_user = MagicMock(return_value=True)
        p.is_user_eligible_for_product = MagicMock(
            side_effect=is_user_elg_for_role
        )
        p.is_user_eligible_for_request = MagicMock(
            side_effect=is_user_elg_for_req
        )
        yield p


@pytest.fixture(scope="function")
def access_manager_main():
    with patch("app.main.AccessManager") as p:
        p.authenticate_user = MagicMock(return_value=True)
        p.is_user_eligible_for_product = MagicMock(
            side_effect=is_user_elg_for_role
        )
        p.is_user_eligible_for_request = MagicMock(
            side_effect=is_user_elg_for_req
        )
        yield p


@pytest.fixture(scope="function")
def file_manager():
    with patch("app.main.FileManager") as p:
        p.prepare_request_for_download_and_get_path = MagicMock(
            return_value="../file.zip"
        )
        yield p


@pytest.fixture(scope="function")
def file_response():
    with patch("app.main.FileResponse") as p:
        inst = MagicMock()
        inst.prepare_request_for_download_and_get_path = MagicMock(
            return_value="../file.zip"
        )
        p.return_value = inst
        yield p


@pytest.fixture(scope="function")
def data_store():
    with patch("app.components.dataset.Datastore") as p:
        inst = MagicMock()
        p.return_value = inst
        inst.dataset_list = MagicMock(return_value=["e-obs", "era5"])
        inst.product_list = MagicMock(side_effect=_get_prods_for_ds)
        inst.product_metadata = MagicMock(side_effect=_get_prods_meta)
        inst.product_details = MagicMock(side_effect=_get_prods_info)
        yield inst


@pytest.fixture(scope="session")
def user_credentials():
    with patch("app.components.dataset.UserCredentials") as p:
        p.return_value = UserCredentials("1:1234")
        yield p()


def _get_prods_for_ds(dataset_id):
    if dataset_id == "e-obs":
        return ["ensemble", "spread"]
    elif dataset_id == "era5":
        return ["reanalysis"]


def _get_prods_info(dataset_id, product_id, use_cache):
    role = _get_prods_meta(dataset_id, product_id)
    return {"metadata": role}


def _get_prods_meta(dataset_id, product_id):
    # NOTE: simple logic for test purposes
    role = "public"
    if dataset_id == "e-obs":
        if product_id == "ensemble":
            role = "internal"
        elif product_id == "spread":
            role = "admin"
    return {"role": role}


def is_user_elg_for_role(user_credentials, product_role_name):
    # NOTE: simple logic for test purposes
    user_role_name = "internal" if user_credentials.id == 1 else "public"
    if product_role_name == "public":
        return True
    if user_credentials.is_public:
        return False
    if user_role_name == "admin":
        return True
    elif user_role_name == product_role_name:
        return True
    else:
        return False


def is_user_elg_for_req(user_credentials, request_id):
    # NOTE: simple logic for test purposes
    return user_credentials.id == request_id
