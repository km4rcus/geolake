from __future__ import annotations

import logging
import pika
from fastapi import HTTPException
from geoquery.geoquery import GeoQuery
from db.dbmanager.dbmanager import DBManager

from .access import AccessManager
from ..datastore.datastore import Datastore
from ..util import UserCredentials


class DatasetManager:

    _LOG = logging.getLogger("DatasetManager")

    @classmethod
    def get_eligible_products_for_all_datasets(
        cls,
        user_credentials: UserCredentials,
    ) -> dict[str, list[str]]:
        cls._LOG.debug(
            f"Getting eligible products for user_id: {user_credentials.id}..."
        )
        AccessManager.authenticate_user(user_credentials)
        data_store = Datastore()
        datasets = {}
        for dataset_id in data_store.dataset_list():
            eligible_products_for_dataset = (
                DatasetManager.get_eligible_products_for_dataset(
                    user_credentials=user_credentials, dataset_id=dataset_id
                )
            )
            if len(eligible_products_for_dataset):
                datasets[dataset_id] = eligible_products_for_dataset
        return datasets

    @classmethod
    def get_eligible_products_for_dataset(
        cls, user_credentials: UserCredentials, dataset_id: str
    ) -> list[str]:
        cls._LOG.debug(
            f"Getting eligible products for user_id: {user_credentials.id},"
            f" dataset_id: {dataset_id}..."
        )
        AccessManager.authenticate_user(user_credentials)
        data_store = Datastore()
        eligible_products_for_dataset = []
        for product_id in data_store.product_list(dataset_id=dataset_id):
            product_metadata = data_store.product_metadata(
                dataset_id=dataset_id, product_id=product_id
            )
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
            f"Getting details for user_id: {user_credentials.id}, dataset_id:"
            f" {dataset_id}, product_id: {product_id}..."
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
                detail=(
                    f"The user with id: {user_credentials.id} is not"
                    f" authorized to use dataset: {dataset_id} product:"
                    f" {product_id}"
                ),
            )

    @classmethod
    def retrieve_data_and_get_request_id(
        cls,
        user_credentials: UserCredentials,
        dataset_id: str,
        product_id: str,
        query: GeoQuery,
        format: str,
    ):
        AccessManager.authenticate_user(user_credentials)
        if user_credentials.is_public:
            raise HTTPException(
                status_code=401,
                detail=(
                    "Anonymouse user cannot execute queries! Please log in!"
                ),
            )
        broker_conn = pika.BlockingConnection(
            pika.ConnectionParameters(host="broker")
        )
        broker_channel = broker_conn.channel()

        request_id = DBManager().create_request(
            user_id=user_credentials.id,
            dataset=dataset_id,
            product=product_id,
            query=query.json(),
        )

        # TODO: find a separator; for the moment use "\"
        message = f"{request_id}\\{dataset_id}\\{product_id}\\{query.json()}\\{format}"

        broker_channel.basic_publish(
            exchange="",
            routing_key="query_queue",
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )
        broker_conn.close()
        return request_id
