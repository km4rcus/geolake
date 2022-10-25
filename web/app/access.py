from __future__ import annotations

import logging
import requests
import secrets
import jwt
from fastapi import HTTPException

from db.dbmanager.dbmanager import DBManager

from .datastore.datastore import Datastore
from .util import UserCredentials


class AccessManager:

    _LOG = logging.getLogger("AccessManager")
    _DATASTORE = Datastore()

    @classmethod
    def is_user_eligible_for_product(
        cls,
        user_credentials: UserCredentials,
        user_role_name: str,
        product_role_name: str,
    ):
        cls._LOG.debug(
            "Verifying eligibility of the user_id:"
            f" {user_credentials.id} against role_name: {product_role_name}"
        )
        if product_role_name == "public":
            return True
        if user_credentials.is_public:
            return False
        if user_role_name == "admin":
            return True
        elif user_role_name == product_role_name:
            return True
        else:
            return False

    @classmethod
    def retrieve_credentials_from_jwt(cls, authorization) -> UserCredentials:
        cls._LOG.debug(f"Getting credentials based on JWT...")
        r = requests.get("https://auth01.cmcc.it/realms/DDS")
        keycloak_public_key = f"""-----BEGIN PUBLIC KEY-----
    {r.json()['public_key']}
    -----END PUBLIC KEY-----"""
        if not authorization:
            cls._LOG.warning("`authorization` header is empty!")
            raise HTTPException(
                status_code=400,
                detail="`authorization` header is empty!",
            )
        token = authorization.split(" ")[-1]
        user_id = jwt.decode(token, keycloak_public_key, audience="account")[
            "sub"
        ]
        # NOTE: if user with `user_id` is defined with DB,
        # we claim authorization was successful
        if user_details := DBManager().get_user_details(user_id):
            return UserCredentials(
                user_id=user_id, user_token=user_details.api_key
            )
        else:
            cls._LOG.info(f"No user found for id `{user_id}`")
            raise HTTPException(
                status_code=401,
                detail="User is not authorized!",
            )

    @classmethod
    def get_datasets_and_eligible_products_names(
        cls, user_credentials: UserCredentials
    ) -> dict:
        cls._LOG.debug("Getting all eligible products for datasets...")
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        datasets = []
        for dataset_id in cls._DATASTORE.dataset_list():
            cls._LOG.debug(
                f"Getting info and eligible products for `{dataset_id}`"
            )
            dataset_info = cls._DATASTORE.dataset_info(
                dataset_id=dataset_id, use_cache=True
            )
            if (
                products := cls.get_eligible_prod_name_and_desc(
                    dataset_id=dataset_id,
                    role=user_role_name,
                    user_credentials=user_credentials,
                )
                > 0
            ):
                dataset_info["products"] = products
                datasets.append(dataset_info)
                continue
            else:
                cls._LOG.debug(
                    f"no eligible products for dataset `{dataset_id}` for the"
                    f" user `{user_credentials.id}`. dataset skipped"
                )
        return datasets

    @classmethod
    def get_eligible_prod_name_and_desc(
        cls, dataset_id: str, role: str, user_credentials: UserCredentials
    ):
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
                if cls.is_user_eligible_for_product(
                    user_credentials=user_credentials,
                    user_role_name=role,
                    product_role_name=metadata.get("role", "public"),
                ):
                    eligible_products.append(
                        {
                            "id": prod_name,
                            "description": prod.get("description"),
                        }
                    )
        return eligible_products

    @classmethod
    def get_eligible_datasets(cls, user_credentials: UserCredentials) -> dict:
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        data_store = Datastore()
        datasets = {}
        for dataset_id in data_store.dataset_list():
            cls._LOG.debug(f"Getting eligible products for `{dataset_id}`")
            eligible_products_for_dataset = (
                cls.get_details_for_eligible_products_for_dataset(
                    dataset_id=dataset_id,
                    user_credentials=user_credentials,
                    user_role_name=user_role_name,
                )["products"].keys()
            )
            if len(eligible_products_for_dataset):
                datasets[dataset_id] = eligible_products_for_dataset
        return datasets

    @classmethod
    def get_details_for_eligible_products_for_dataset(
        cls,
        dataset_id: str,
        user_credentials: UserCredentials,
        user_role_name: None | str = None,
    ) -> dict:
        cls._LOG.debug(
            f"Getting details for eligible products of `{dataset_id}`..."
        )
        if not user_role_name:
            user_role_name = DBManager().get_user_role_name(
                user_credentials.id
            )
        details = Datastore().dataset_info(
            dataset_id=dataset_id, use_cache=True
        )
        eligible_products = {}
        if (products := details.get("products")) and isinstance(
            products, dict
        ):
            for prod_name, prod in products.items():
                assert (
                    "metadata" in prod
                ), f"Metadata are not defined for the product `{prod_name}`"
                metadata = prod["metadata"]
                if cls.is_user_eligible_for_product(
                    user_credentials=user_credentials,
                    user_role_name=user_role_name,
                    product_role_name=metadata.get("role", "public"),
                ):
                    eligible_products[prod_name] = prod
        details["products"] = eligible_products
        return details

    @classmethod
    def get_details_for_product_if_eligible(
        cls,
        dataset_id: str,
        product_id: str,
        user_credentials: UserCredentials,
    ) -> dict:
        cls._LOG.debug(
            f"Getting details for eligible products of `{dataset_id}`..."
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
                    f"the product `{dataset_id}.{product_id}` was not found!"
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
                user_credentials=user_credentials,
                user_role_name=user_role_name,
                product_role_name=prod["metadata"].get("role", "public"),
            ):
                details["products"] = {product_id: prod}
            else:
                cls._LOG.info(
                    f"user `{user_credentials.id}` was not authorized for"
                    f" product `{dataset_id}.{product_id}`"
                )
                raise HTTPException(
                    status_code=401,
                    detail="User is not authorized!",
                )
        details["products"] = eligible_products
        return details
