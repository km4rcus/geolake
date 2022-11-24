"""Module with tools for dataset and details retrieving"""
from __future__ import annotations

import os
import logging
import pika
from fastapi import HTTPException

from geoquery.geoquery import GeoQuery
from db.dbmanager.dbmanager import DBManager

from .access import AccessManager
from .meta import LoggableMeta
from ..datastore.datastore import Datastore
from ..util import UserCredentials, log_execution_time


class DatasetManager(metaclass=LoggableMeta):
    """Manager that handles dataset present in the geokube-dds catalog"""

    _LOG = logging.getLogger("DatasetManager")

    @classmethod
    def assert_product_exists(cls, dataset_id, product_id: None | str = None):
        """Assert that the dataset or product exist.
        If `product_id` is set, the method verifies if it belongs to the
        dataset with ID in `dataset_id`.

        Parameters
        ----------
        dataset_id : str
            ID of the dataset
        product_id : str, optional
            ID of the product

        Raises
        -------
        HTTPException
            400 if:
                a) dataset with `dataset_id` does not exist,
                b) product for the given dataset does not exist
                c) product might exist, but it does not belong to the dataset `dataset_id`
        """
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
    @log_execution_time(_LOG)
    def get_eligible_products_for_all_datasets(
        cls,
        user_credentials: UserCredentials,
    ) -> dict[str, list[str]]:
        """Get eligible products for all datasets defined in the catalog.

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user

        Returns
        -------
        products : dict
            The dictionary of datasets and products in the form:
            ```python
            {
                dataset_id_1: [prod_id_1, prod_id_2, ...],
                dataset_id_2: ...
            }
            ```

        Raises
        -------
        HTTPException
            400 if user does not exist or the key is not valid
        """
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
    @log_execution_time(_LOG)
    def get_eligible_products_for_dataset(
        cls, user_credentials: UserCredentials, dataset_id: str
    ) -> list[str]:
        """Get eligible products only for the dataset with id in `dataset_id`.

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user

        Returns
        -------
        products : list
            The dictionary of datasets and products in the form:
            ```python
            [prod_id_1, prod_id_2, ...]
            ```

        Raises
        -------
        HTTPException
            400 if user does not exist or the key is not valid
        """
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
    @log_execution_time(_LOG)
    def get_details_if_product_eligible(
        cls,
        user_credentials: UserCredentials,
        dataset_id: str,
        product_id: str,
    ) -> dict:
        """Get details for the given product, if the user is eligible

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the product

        Returns
        -------
        details : dict
            The dictionary with details of the product

        Raises
        -------
        HTTPException
            400 if user does not exist or the key is not valid
            401 if the user is not authorized for the given product
        """
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
    @log_execution_time(_LOG)
    def retrieve_data_and_get_request_id(
        cls,
        user_credentials: UserCredentials,
        dataset_id: str,
        product_id: str,
        query: GeoQuery,
        format: str,
    ):
        """Query the data and return the ID of the request.

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the product
        query : GeoQuery
            Query to perform
        format : str
            Format of the data

        Returns
        -------
        request_id : int
            ID of the request

        Raises
        -------
        HTTPException
            400 if user does not exist or the key is not valid
            401 if anonymous user attempts to execute a query
        """
        AccessManager.authenticate_user(user_credentials)
        if user_credentials.is_public:
            cls._LOG.info("attempt to execute query by an anonymous user!")
            raise HTTPException(
                status_code=401,
                detail="Anonymouse user cannot execute queries!",
            )
        broker_conn = pika.BlockingConnection(
            pika.ConnectionParameters(host="broker")
        )
        broker_channel = broker_conn.channel()

        request_id = DBManager().create_request(
            user_id=user_credentials.id,
            dataset=dataset_id,
            product=product_id,
            query=query.original_query_json(),
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
    @log_execution_time(_LOG)
    def estimate(
        cls,
        dataset_id: str,
        product_id: str,
        query: GeoQuery,
        unit: str | None,
    ):
        """Estimate the size of the resulting data.
        No authentication is needed for estimation query.

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the product
        query : GeoQuery
            Query to perform
        unit : str
            One of unit [bytes, kB, MB, GB] to present the result. If `None`,
            unit will be inferred.

        Returns
        -------
        size_details : dict
            Estimated size of  the query in the form:
            ```python
            {
                "value": val,
                "units": units
            }
            ```
        """
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
        return (
            _make_bytes_readable_dict(size_bytes=query_bytes_estimation)
            if unit is None
            else _convert_bytes(query_bytes_estimation, unit)
        )


def _convert_bytes(size_bytes: int, unit: str) -> float:
    unit = unit.lower()
    if unit == "kb":
        value = size_bytes / 1024
    elif unit == "mb":
        value = size_bytes / 1024**2
    elif unit == "gb":
        value = size_bytes / 1024**3
    else:
        raise ValueError(f"unsupported unit: {unit}")
    if (value := round(value, 2)) == 0.00:
        value = 0.01
    return {"value": value, "units": unit}


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
    if (val := round(val, 2)) == 0.00:
        val = 0.01
    return {"value": val, "units": units}
