from __future__ import annotations

import logging
from fastapi import HTTPException

from .access import AccessManager
from ..datastore.datastore import Datastore
from ..util import UserCredentials


class DatasetManager:

    _LOG = logging.getLogger("DatasetManager")

    @classmethod
    def get_eligible_products_for_all_datasets(
        cls, user_credentials: UserCredentials,
    ) -> dict[str, list[str]]:
        cls._LOG.debug(
            f"Getting eligible products for user_id: {user_credentials.id}..."
        )
        AccessManager.authenticate_user(user_credentials)
        data_store = Datastore()
        datasets = {}
        for dataset_id in data_store.dataset_list():
            eligible_products_for_dataset = DatasetManager.get_eligible_products_for_dataset(
                user_credentials=user_credentials, dataset_id=dataset_id
            )
            if len(eligible_products_for_dataset):
                datasets[dataset_id] = eligible_products_for_dataset
        return datasets

    @classmethod
    def get_eligible_products_for_dataset(
        cls, user_credentials: UserCredentials, dataset_id: str
    ) -> list[str]:
        cls._LOG.debug(
            f"Getting eligible products for user_id: {user_credentials.id}, dataset_id: {dataset_id}..."
        )
        AccessManager.authenticate_user(user_credentials)
        data_store = Datastore()
        eligible_products_for_dataset = []
        for product_id in data_store.product_list(dataset_id=dataset_id):
            product_metadata = data_store.product_metadata(
                dataset_id=dataset_id, product_id=product_id
            )
            raise RuntimeError(product_metadata)
            if AccessManager.is_user_eligible_for_role(
                user_credentials=user_credentials,
                product_role_name=product_metadata.get("role"),
            ):
                eligible_products_for_dataset.append(product_id)
        return eligible_products_for_dataset

    @classmethod
    def get_details_if_product_eligible(
        cls,
        user_credentials: UserCredentials,
        dataset_id: str,
        product_id: str,
    ) -> list[str]:
        cls._LOG.debug(
            f"Getting details for user_id: {user_credentials.id}, dataset_id: {dataset_id}, product_id: {product_id}..."
        )
        AccessManager.authenticate_user(user_credentials)
        data_store = Datastore()
        product_details = data_store.product_info(
            dataset_id=dataset_id, product_id=product_id
        )
        if AccessManager.is_user_eligible_for_role(
            user_credentials=user_credentials,
            product_role_name=product_details.get("metadata", {}).get("role"),
        ):
            return product_details
        else:
            raise HTTPException(
                status_code=401,
                detail=f"The user with id: {user_credentials.id} is not authorized to use dataset: {dataset_id} product: {product_id}",
            )
