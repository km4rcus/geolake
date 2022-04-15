from __future__ import annotations

import logging
from fastapi import HTTPException

from .access import AccessManager
from ..datastore.datastore import Datastore


class DatasetManager:

    _LOG = logging.getLogger("DatasetManager")

    @classmethod
    def get_eligible_products_names_for_role(
        cls, role: Role | None = None
    ) -> dict[str, list[str]]:
        data_store = Datastore()
        eligible_datasets = {}
        for dataset_id in data_store.dataset_list():
            eligible_products_for_dataset = []
            for product_id in data_store.product_list(dataset_id=dataset_id):
                product_details = data_store.product_metadata(
                    dataset_id=dataset_id, product_id=product_id
                )
                if AccessManager.is_user_role_eligible(
                    user_role_name=role.role_name,
                    product_role_name=product_details.get("role"),
                ):
                    eligible_products_for_dataset.append(product_id)
            if len(eligible_products_for_dataset):
                eligible_datasets[dataset_id] = eligible_products_for_dataset
        return eligible_datasets

    @classmethod
    def get_details_if_dataset_eligible(
        cls, dataset_id: str, role_name: str | None = None
    ) -> dict[str, dict]:
        cls._LOG.info(f"Getting details of dataset: {dataset_id}")
        data_store = Datastore()
        eligible_products_for_dataset = {}
        for product_id in data_store.product_list(dataset_id=dataset_id):
            product_details = data_store.product_info(
                dataset_id=dataset_id, product_id=product_id
            )
            if AccessManager.is_user_role_eligible(
                user_role_name=role_name,
                product_role_name=product_details.get("metadata", {}).get(
                    "role", "public"
                ),
            ):
                eligible_products_for_dataset[product_id] = product_details
        return eligible_products_for_dataset

    @classmethod
    def get_details_if_product_eligible(
        cls, dataset_id: str, product_id: str, role_name: str | None = None
    ) -> dict[str, dict] | None:
        cls._LOG.debug(
            f"Getting details of dataset: {dataset_id} and product: {product_id}"
        )
        data_store = Datastore()
        product_details = data_store.product_info(
            dataset_id=dataset_id, product_id=product_id
        )
        if AccessManager.is_user_role_eligible(
            user_role_name=role_name,
            product_role_name=product_details.get("metadata", {}).get(
                "role", "public"
            ),
        ):
            return product_details
        cls._LOG.debug(
            f"Role {role_name} is not valid for dataset: {dataset_id} product: {product_id}"
        )
        raise HTTPException(
            status_code=401,
            detail=f"You are not authorized to use dataset: {dataset_id} product: {product_id}",
        )
