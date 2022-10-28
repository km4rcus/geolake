"""Module contains the class DatasetManager being responsible for
accessing data via datastore.Datastore and scheduling the request job"""
from __future__ import annotations

import logging

from fastapi import HTTPException
import pika

from geoquery.geoquery import GeoQuery
from db.dbmanager.dbmanager import DBManager

from .datastore.datastore import Datastore
from .util import UserCredentials, log_execution_time
from .access import AccessManager


class DatasetManager:
    """The component for dataset management, including getting details
    and submitting a request"""

    _LOG = logging.getLogger("DatasetManager")
    _LOG.setLevel(logging.DEBUG)
    _LOG.addHandler(logging.StreamHandler())
    _DATASTORE = Datastore()

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
        """
        cls._LOG.debug("getting all eligible products for datasets...")
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        datasets = []
        for dataset_id in cls._DATASTORE.dataset_list():
            if dataset_id == "visir":
                cls._LOG.info(
                    "skipping `visir` dataset due to theerror geokube/#253"
                )
                continue
            cls._LOG.debug(
                "getting info and eligible products for `%s`", dataset_id
            )
            dataset_info = cls._DATASTORE.dataset_info(dataset_id=dataset_id)
            datasets.append(
                cls._get_dataset_information_from_details_dict(
                    dataset_dict=dataset_info,
                    user_role_name=user_role_name,
                    dataset_id=dataset_id,
                    user_credentials=user_credentials,
                )
            )
        return datasets

    @classmethod
    @log_execution_time(_LOG)
    def get_details_for_dataset_products_if_eligible(
        cls,
        dataset_id: str,
        user_credentials: UserCredentials,
    ) -> dict:
        """Get details for the given product indicated by `dataset_id`
        and `product_id` arguments.

        Parameters
        ----------
        dataset_id : str
            ID of the dataset
        user_credentials : UserCredentials
            Current user credentials

        Returns
        -------
        details : dict
            Details for the given product

        Raises
        -------
        HTTPException
            400 if dataset details does not contain `products` key
        """
        cls._LOG.debug(
            "getting details for eligible products of `%s`", dataset_id
        )
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        details = Datastore().dataset_details(
            dataset_id=dataset_id, use_cache=True
        )
        if (products := details.get("products")) and isinstance(
            products, dict
        ):
            return cls._get_dataset_information_from_details_dict(
                dataset_dict=details,
                user_role_name=user_role_name,
                dataset_id=dataset_id,
                user_credentials=user_credentials,
            )
        cls._LOG.error(
            "dataset `%s` details does not contain `products` key", dataset_id
        )
        raise HTTPException(
            status_code=400,
            detail=(
                f"dataset `{dataset_id}` details does not contain"
                " `products` key"
            ),
        )

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
        except KeyError:
            cls._LOG.info(
                "dataset `%s` does not have products defined", dataset_id
            )
        else:
            if len(eligible_prods) == 0:
                cls._LOG.debug(
                    "no eligible products for dataset `%s` for the user"
                    " `%s`. dataset skipped",
                    dataset_id,
                    user_credentials.id,
                )
        finally:
            dataset_dict["products"] = eligible_prods
        return dataset_dict

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
        """Schedule a query and return an ID of the request.

        Parameters
        ----------
        user_credentials : UserCredentials
            Current user credentials
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the product for the dataset `dataset_id`
        query :  GeoQuery
            An object representing a query to execute
        format : str
            The format of the resulting file

        Returns
        -------
        request_id : int
            An ID of the scheduled request
        """
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

    ######################################################
