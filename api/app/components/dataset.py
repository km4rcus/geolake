from __future__ import annotations

import os
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
    def assert_product_exists(cls, dataset_id, product_id: None | str = None):
        dset = Datastore(cache_path="/cache")
        if dataset_id not in dset.dataset_list():
            cls._LOG.info(
                "requested dataset: `%s` was not found in the catalog!",
                dataset_id,
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Dataset with id `{dataset_id}` does not exist in the"
                    " catalog!"
                ),
            )
        if product_id is not None:
            if product_id not in dset.product_list(dataset_id):
                cls._LOG.info(
                    "requested product: `%s` for dataset: `%s` was not found"
                    " in the catalog!",
                    product_id,
                    dataset_id,
                )
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Product with id `{product_id}` does not exist for"
                        f" the dataset with id `{dataset_id}`!"
                    ),
                )

    @classmethod
    def get_eligible_products_for_all_datasets(
        cls,
        user_credentials: UserCredentials,
    ) -> dict[str, list[str]]:
        cls._LOG.debug(
            "getting eligible products for user_id: `%s`", user_credentials.id
        )
        AccessManager.authenticate_user(user_credentials)
        data_store = Datastore(cache_path="/cache")
        datasets = {}
        for dataset_id in data_store.dataset_list():
            eligible_products_for_dataset = (
                DatasetManager.get_eligible_products_for_dataset(
                    user_credentials=user_credentials, dataset_id=dataset_id
                )
            )
            if len(eligible_products_for_dataset) > 0:
                datasets[dataset_id] = eligible_products_for_dataset
        return datasets

    @classmethod
    def get_eligible_products_for_dataset(
        cls, user_credentials: UserCredentials, dataset_id: str
    ) -> list[str]:
        cls._LOG.debug(
            "getting eligible products for user_id: `%s`, dataset_id: `%s`",
            user_credentials.id,
            dataset_id,
        )
        AccessManager.authenticate_user(user_credentials)
        data_store = Datastore(cache_path="/cache")
        eligible_products_for_dataset = []
        cls.assert_product_exists(dataset_id=dataset_id)
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
            "getting details for user_id: `%s`, dataset_id: `%s`, product_id:"
            " `%s`",
            user_credentials.id,
            dataset_id,
            product_id,
        )
        AccessManager.authenticate_user(user_credentials)
        data_store = Datastore(cache_path="/cache")
        cls.assert_product_exists(dataset_id=dataset_id, product_id=product_id)
        product_details = data_store.product_info(
            dataset_id=dataset_id, product_id=product_id, use_cache=True
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
            cls._LOG.info("attempt to execute query by an anonymous user!")
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

    @classmethod
    def estimate(
        cls,
        dataset_id: str,
        product_id: str,
        query: GeoQuery,
    ):
        try:
            query_bytes_estimation = (
                Datastore(cache_path="/cache")
                .query(dataset_id, product_id, query, compute=False)
                .nbytes
            )
        except KeyError as exception:
            cls._LOG.error(
                "dataset `%s` or product `%s` does not exist!",
                dataset_id,
                product_id,
                exc_info=True,
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Dataset `{dataset_id}` or product `{product_id}` does"
                    " not exist!"
                ),
            ) from exception
        return _make_bytes_readable_dict(size_bytes=query_bytes_estimation)


def _make_bytes_readable_dict(size_bytes: int) -> dict:
    units = "bytes"
    val = size_bytes
    if val > 1024:
        units = "kB"
        val /= 1024
    if val > 1024:
        units = "MB"
        val /= 1024
    if val > 1024:
        units = "GB"
        val /= 1024
    return {"value": val, "units": units}
