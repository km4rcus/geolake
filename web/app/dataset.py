from __future__ import annotations

import logging

from fastapi import HTTPException
import pika

from geoquery.geoquery import GeoQuery
from db.dbmanager.dbmanager import DBManager

from .datastore.datastore import Datastore
from .util import UserCredentials
from .access import AccessManager


class DatasetManager:
    """The component for dataset management, including getting details
    and submitting a request"""

    _LOG = logging.getLogger("DatasetManager")
    _DATASTORE = Datastore()

    @classmethod
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
            cls._LOG.debug(
                "getting info and eligible products for `%s`", dataset_id
            )
            dataset_info = cls._DATASTORE.dataset_info(
                dataset_id=dataset_id, use_cache=True
            )
            if (
                len(
                    products := cls.get_eligible_prod_name_and_desc(
                        dataset_id=dataset_id,
                        role=user_role_name,
                    )
                )
                > 0
            ):
                dataset_info["products"] = products
                datasets.append(dataset_info)
                continue
            cls._LOG.debug(
                "no eligible products for dataset `%s` for the user `%s`."
                " dataset skipped",
                dataset_id,
                user_credentials.id,
            )
        return datasets

    @classmethod
    def get_eligible_prod_name_and_desc(cls, dataset_id: str, role: str) -> list:
        """Get names and descriptions of products for the dataset
        `dataset_id` eligible for the role indicated by the `role` argument.

        Parameters
        ----------
        dataset_id : str
            ID of ad dataset
        role :  str
            Role against which eligibility of products is verified

        Returns
        -------
        products : list
            A list of eligible products for the datasets `dataset_id`
            for the role `role`
        """
        eligible_products = []
        details = cls._DATASTORE.dataset_info(
            dataset_id=dataset_id, use_cache=True
        )
        if (products := details.get("products")) and isinstance(
            products, dict
        ):
            for prod_name, prod in products.items():
                assert (
                    "metadata" in prod
                ), f"Metadata are not defined for the product `{prod_name}`"
                metadata = prod["metadata"]
                if AccessManager.is_user_eligible_for_product(
                    product_role_name=metadata.get("role"),
                    user_role_name=role,
                ):
                    eligible_products.append(
                        {
                            "id": prod_name,
                            "description": prod.get("description"),
                        }
                    )
        return eligible_products

    @classmethod
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

        Returns
        -------
        request_id : int
            An ID of the scheduled request
        """
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

    ######################################################
    @classmethod
    def get_details_for_product_if_eligible(
        cls,
        dataset_id: str,
        product_id: str,
        user_credentials: UserCredentials,
    ) -> dict:
        cls._LOG.debug(
            "getting details for eligible products of `%s`", dataset_id
        )
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        details = Datastore().dataset_info(
            dataset_id=dataset_id, use_cache=True
        )
        eligible_products = {}
        if (products := details.get("products")) and isinstance(
            products, dict
        ):
            if product_id not in products:
                cls._LOG.info(
                    "the product `{%s}.{%s}` was not found!",
                    dataset_id,
                    product_id,
                )
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"the product `{dataset_id}.{product_id}` was not"
                        " found!"
                    ),
                )
            prod = products[product_id]
            assert (
                "metadata" in prod
            ), f"Metadata are not defined for the product `{product_id}`"
            if cls.is_user_eligible_for_product(
                product_role_name=prod["metadata"].get("role"),
                user_role_name=user_role_name,
            ):
                details["products"] = {product_id: prod}
            else:
                cls._LOG.info(
                    "user `%s` was not authorized for product `%s.%s`",
                    user_credentials.id,
                    dataset_id,
                    product_id,
                )
                raise HTTPException(
                    status_code=401,
                    detail="User is not authorized!",
                )
        details["products"] = eligible_products
        return details
