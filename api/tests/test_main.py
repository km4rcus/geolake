import pytest
from unittest.mock import ANY, MagicMock, Mock, patch


from .fixture import (
    access_manager,
    access_manager_main,
    client,
    data_store,
    file_manager,
    user_credentials,
    file_response,
)


def test_get_only_eligible_datasets(
    client, access_manager, data_store, user_credentials
):
    response = client.get("/datasets", headers={"User-Token": "1:1234"}).json()
    data_store.dataset_list.assert_called_once()
    access_manager.is_user_eligible_for_role.assert_any_call(
        user_credentials=ANY, product_role_name="public"
    )
    access_manager.is_user_eligible_for_role.assert_any_call(
        user_credentials=ANY, product_role_name="internal"
    )
    access_manager.authenticate_user.assert_called()
    data_store.dataset_list.assert_called_once()
    data_store.product_list.assert_any_call(dataset_id="e-obs")
    data_store.product_list.assert_any_call(dataset_id="era5")
    assert "e-obs" in response
    assert response["e-obs"] == ["ensemble"]
    assert "era5" in response
    assert response["era5"] == ["reanalysis"]


def test_get_only_eligible_products_for_dataset(
    client, access_manager, data_store, user_credentials
):
    response = client.get("/datasets/era5").json()
    access_manager.authenticate_user.assert_called_once()
    data_store.product_list.assert_any_call(dataset_id="era5")
    data_store.product_metadata.assert_any_call(
        dataset_id="era5", product_id="reanalysis"
    )
    assert response == ["reanalysis"]

    response = client.get(
        "datasets/e-obs", headers={"User-Token": "1:1234"}
    ).json()
    access_manager.authenticate_user.assert_called()
    data_store.product_list.assert_any_call(dataset_id="e-obs")
    data_store.product_metadata.assert_any_call(
        dataset_id="e-obs", product_id="ensemble"
    )
    data_store.product_metadata.assert_any_call(
        dataset_id="e-obs", product_id="spread"
    )
    assert response == ["ensemble"]


def test_fail_to_get_details_for_not_authorized_user_for_admin_product(
    client, access_manager, data_store, user_credentials
):
    response = client.get(
        "/datasets/e-obs/spread", headers={"User-Token": "1:1234"}
    )
    access_manager.authenticate_user.assert_called_once()
    access_manager.is_user_eligible_for_role.assert_any_call(
        user_credentials=ANY, product_role_name="admin"
    )
    assert response.status_code == 401
    assert response.json()["detail"].startswith("The user with id")


def test_fail_to_get_details_for_not_authorized_user_for_internal_product(
    client, access_manager, data_store, user_credentials
):
    response = client.get("/datasets/e-obs/ensemble")
    access_manager.authenticate_user.assert_called_once()
    access_manager.is_user_eligible_for_role.assert_any_call(
        user_credentials=ANY, product_role_name="internal"
    )
    assert response.status_code == 401
    assert response.json()["detail"].startswith("The user with id")


def test_get_details_for_authorized_user_for_public_product(
    client, access_manager, data_store, user_credentials
):
    response = client.get(
        "/datasets/era5/reanalysis", headers={"User-Token": "1:1234"}
    )
    access_manager.authenticate_user.assert_called_once()
    access_manager.is_user_eligible_for_role.assert_any_call(
        user_credentials=ANY, product_role_name="public"
    )
    assert response.status_code == 200


def test_get_details_for_authorized_user_for_internal_product(
    client,
    access_manager,
    data_store,
    file_manager,
    file_response,
    user_credentials,
):
    response = client.get(
        "/datasets/e-obs/ensemble", headers={"User-Token": "1:1234"}
    )
    access_manager.authenticate_user.assert_called_once()
    access_manager.is_user_eligible_for_role.assert_any_call(
        user_credentials=ANY, product_role_name="internal"
    )
    assert response.status_code == 200


def test_download_request_fail_for_other_user(
    client,
    access_manager_main,
    data_store,
    file_manager,
    file_response,
    user_credentials,
):
    response = client.get("/download/1", headers={"User-Token": "2:1234"})
    access_manager_main.authenticate_user.assert_called_once()
    access_manager_main.is_user_eligible_for_request.assert_called_once_with(
        user_credentials=ANY, request_id=1
    )
    file_manager.prepare_request_for_download_and_get_path.assert_not_called()
    assert response.status_code == 401
    assert (
        response.json()["detail"]
        == "User with id: 2 is not authorized for results of the request with"
        " id 1"
    )


def test_download_fail_for_anonymous_user(
    client,
    access_manager_main,
    data_store,
    file_manager,
    file_response,
    user_credentials,
):
    response = client.get("/download/1")
    access_manager_main.authenticate_user.assert_called_once()
    access_manager_main.is_user_eligible_for_request.assert_called_once_with(
        user_credentials=ANY, request_id=1
    )
    file_manager.prepare_request_for_download_and_get_path.assert_not_called()
    assert response.status_code == 401
    assert (
        response.json()["detail"]
        == "User with id: None is not authorized for results of the request"
        " with id 1"
    )


def test_successful_download_for_authorized_user(
    client,
    access_manager_main,
    data_store,
    file_manager,
    file_response,
    user_credentials,
):
    response = client.get("/download/1", headers={"User-Token": "1:1234"})
    access_manager_main.authenticate_user.assert_called_once()
    access_manager_main.is_user_eligible_for_request.assert_called_once_with(
        user_credentials=ANY, request_id=1
    )
    file_manager.prepare_request_for_download_and_get_path.assert_called_once_with(
        request_id=1
    )
    assert response.status_code == 200


def test_fail_to_execute_query_if_not_authenticated(
    client, access_manager, user_credentials
):
    response = client.post(
        "datasets/era5/reanalysis/execute",
        json={
            "variable": ["mean_air_temperature"],
            "locations": {"latitude": 10, "longitude": 25},
        },
    )
    access_manager.authenticate_user.assert_called_once()
    assert response.status_code == 401
    assert (
        response.json()["detail"]
        == "Anonymouse user cannot execute queries! Please log in!"
    )


def test_fail_to_get_requests_details_for_anonymous_user(
    client, access_manager
):
    response = client.get("requests")
    assert response.status_code == 401
    assert response.json()["detail"] == "Anonymous user doesn't have requests!"
