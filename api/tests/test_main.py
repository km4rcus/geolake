import pytest
from unittest.mock import ANY, MagicMock, Mock, patch


from .fixture import access_manager, client, data_store, user_credentials


def test_get_only_eligible_datasets(
    client, access_manager, data_store, user_credentials
):
    response = client.get("/datasets")
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
    assert not user_credentials().is_public
    assert access_manager.is_user_eligible_for_role(
        user_credentials(), "public"
    )
    assert access_manager.is_user_eligible_for_role(
        user_credentials(), "internal"
    )
    assert not access_manager.is_user_eligible_for_role(
        user_credentials(), "admin"
    )


def test_get_only_eligible_products_for_dataset():
    pass


def test_fail_to_get_details_for_not_authorized_user():
    pass


def test_get_request_status_without_authentication():
    pass


def test_fail_to_execute_query_if_not_authenticated():
    pass
