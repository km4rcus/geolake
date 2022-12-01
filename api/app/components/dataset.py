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
from ..exceptions import (
    NoEligibleProductInDatasetError,
    MissingKeyInCatalogEntryError,
)


class DatasetManager(metaclass=LoggableMeta):
    """Manager that handles dataset present in the geokube-dds catalog"""

    _LOG = logging.getLogger("geokube.DatasetManager")
    _DATASTORE = Datastore(cache_path="/cache")

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
        if dataset_id not in cls._DATASTORE.dataset_list():
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
            if product_id not in cls._DATASTORE.product_list(dataset_id):
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
    def get_datasets_and_eligible_products_names(
        cls, user_credentials: UserCredentials
    ) -> list:
        """Get datasets names, their metadata and products names (if eligible for a user).
        If no eligible products are found for a dataset, it is not included.

        Parameters
        ----------
        user_credentials : UserCredentials
            Current user credentials

        Returns
        -------
        datasets : list
            A list of datasets information (including metadata and
            eligible products lists)

        Raises
        -------
        MissingKeyInCatalogEntryError
            If the dataset catalog entry does not contain the required key
        """
        cls._LOG.debug("getting all eligible products for datasets...")
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        datasets = []
        for dataset_id in cls._DATASTORE.dataset_list():
            if dataset_id == "visir":
                cls._LOG.info(
                    "skipping `visir` dataset due to the error geokube/#253"
                )
                continue
            cls._LOG.debug(
                "getting info and eligible products for `%s`", dataset_id
            )
            dataset_info = cls._DATASTORE.dataset_info(dataset_id=dataset_id)
            try:
                datasets.append(
                    cls._get_dataset_information_from_details_dict(
                        dataset_dict=dataset_info,
                        user_role_name=user_role_name,
                        dataset_id=dataset_id,
                        user_credentials=user_credentials,
                    )
                )
            except NoEligibleProductInDatasetError:
                cls._LOG.debug(
                    f"dataset '{dataset_id}' will not be considered. no"
                    " eligible products for the user role name"
                    f" '{user_role_name}'"
                )
                continue
        return datasets

    @classmethod
    @log_execution_time(_LOG)
    def get_details_for_product_if_eligible(
        cls,
        dataset_id: str,
        product_id: str,
        user_credentials: UserCredentials,
    ) -> dict:
        """Get details for the given product indicated by `dataset_id`
        and `product_id` arguments.

        Parameters
        ----------
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the dataset
        user_credentials : UserCredentials
            Current user credentials

        Returns
        -------
        details : dict
            Details for the given product

        Raises
        -------
        MissingKeyInCatalogEntryError
            If the dataset catalog entry does not contain the required key
        """
        cls._LOG.debug(
            "getting details for eligible products of `%s`", dataset_id
        )
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        details = cls._DATASTORE.product_details(
            dataset_id=dataset_id, product_id=product_id, use_cache=True
        )
        AccessManager.assert_is_role_eligible(
            product_role_name=details["metadata"].get("role"),
            user_role_name=user_role_name,
        )
        return details

    @classmethod
    @log_execution_time(_LOG)
    def _get_dataset_information_from_details_dict(
        cls,
        dataset_dict: dict,
        user_role_name: str,
        dataset_id: str,
        user_credentials: UserCredentials,
    ) -> dict:
        cls._LOG.debug(
            "getting all eligible products for dataset: `%s`", dataset_id
        )
        try:
            eligible_prods = {
                prod_name: prod_info
                for prod_name, prod_info in dataset_dict["products"].items()
                if AccessManager.is_role_eligible_for_product(
                    product_role_name=prod_info.get("role"),
                    user_role_name=user_role_name,
                )
            }
        except KeyError as err:
            cls._LOG.error(
                "dataset `%s` does not have products defined",
                dataset_id,
                exc_info=True,
            )
            raise MissingKeyInCatalogEntryError(
                key="products", dataset=dataset_id
            ) from err
        else:
            if len(eligible_prods) == 0:
                cls._LOG.debug(
                    "no eligible products for dataset `%s` for the user"
                    " `%s`. dataset skipped",
                    dataset_id,
                    user_credentials.id,
                )
                raise NoEligibleProductInDatasetError(
                    dataset_id=dataset_id, user_role_name=user_role_name
                )
            else:
                dataset_dict["products"] = eligible_prods
        return dataset_dict

    @classmethod
    @log_execution_time(_LOG)
    def get_product_metadata(
        cls,
        user_credentials: UserCredentials,
        dataset_id: str,
        product_id: str,
    ):
        cls.assert_product_exists(dataset_id=dataset_id, product_id=product_id)
        return cls._DATASTORE.product_metadata(dataset_id, product_id)

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
        cls._LOG.debug("geoquery: %s", query)
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
            query_bytes_estimation = cls._DATASTORE.query(
                dataset_id, product_id, query, compute=False
            ).nbytes
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
