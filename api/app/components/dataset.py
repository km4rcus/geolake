from __future__ import annotations

import logging
from typing import List
from fastapi import HTTPException

from .role import RoleManager
from ..datastore.datastore import Datastore

class DatasetManager:

    _LOG = logging.getLogger("DatasetManager")

    @classmethod
    def get_eligible_products_names_for_role(cls, role_name: str | None = None) -> Mapping[str, List[str]]:
        data_store = Datastore()
        eligible_datasets = {}
        for dataset_id in data_store.dataset_list():
            eligible_products_for_dataset = []
            for product_id in data_store.product_list(dataset_id=dataset_id):
                product_details = data_store.product_metadata(dataset_id=dataset_id, product_id=product_id)
                if RoleManager.is_role_eligible(product_details.get("role"), role_name):
                    eligible_products_for_dataset.append(product_id)
            if len(eligible_products_for_dataset):
                eligible_datasets[dataset_id] = eligible_products_for_dataset
        return eligible_datasets

    @classmethod
    def get_details_if_dataset_eligible(cls, dataset_id: str, role_name: str | None = None):
        cls._LOG.info(f"Getting details of dataset: {dataset_id}")
        data_store = Datastore()
        eligible_products_for_dataset = {}
        for product_id in data_store.product_list(dataset_id=dataset_id):
            product_details = data_store.product_info(dataset_id=dataset_id, product_id=product_id)
            if RoleManager.is_role_eligible(product_details.get("metadata", {}).get("role"), role_name):
                eligible_products_for_dataset[product_id] = product_details
        return eligible_products_for_dataset

    @classmethod
    def get_details_if_product_eligible(cls, dataset_id: str, product_id: str, role_name: str | None = None):
        cls._LOG.debug(f"Getting details of dataset: {dataset_id} and product: {product_id}")
        data_store = Datastore()
        product_details = data_store.product_info(dataset_id=dataset_id, product_id=product_id)
        if RoleManager.is_role_eligible(product_details.get("metadata", {}).get("role"), role_name):
            return product_details
        cls._LOG.debug(f"Role {role_name} is not valid for dataset: {dataset_id} product: {product_id}")
        raise HTTPException(status_code=401, detail=f"You are not authorized to use dataset: {dataset_id} product: {product_id}") 