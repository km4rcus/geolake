"""Module responsible for authnetication and authorization functionalities"""
from __future__ import annotations

import logging
import requests
import jwt
from fastapi import HTTPException

from db.dbmanager.dbmanager import DBManager

from .util import UserCredentials, log_execution_time


class AccessManager:
    """The component for managing access to data, authentication, and
    authorization of a user"""

    _LOG = logging.getLogger("AccessManager")

    @classmethod
    @log_execution_time(_LOG)
    def is_role_eligible_for_product(
        cls,
        product_role_name: str | None = None,
        user_role_name: str | None = None,
    ):
        """Check if given role is eligible for credentials based on JWT token
         or public profile, if `authorization` header is not provided.

        Parameters
        ----------
        product_role_name : str, optional, default=None
            The role which is eligible for the given product.
            If `None`, product_role_name is claimed to be public
        user_role_name: str, optional, default=None
            The role of a user. If `None`, user_role_name is claimed
            to be public

        Returns
        -------
        is_eligible : bool
            Flag which indicate if the given `user_role_name` is eligible
             for the product with `product_role_name`
        """
        cls._LOG.debug(
            "verifying eligibility of the product role: %s against"
            " role_name %s",
            product_role_name,
            user_role_name,
        )
        return True
        if product_role_name == "public" or product_role_name is None:
            return True
        if user_role_name is None:
            # NOTE: it means, we consider the public profile
            return False
        if user_role_name == "admin":
            return True
        if user_role_name == product_role_name:
            return True
        return False

    @classmethod
    @log_execution_time(_LOG)
    def retrieve_credentials_from_jwt(cls, authorization) -> UserCredentials:
        """Get credentials based on JWT token or public profile,
        if `authorization` header is not provided.

        Parameters
        ----------
        authorization : str
            Value of a request header with name `Authorization`

        Returns
        -------
        user_credentials : UserCredentials
            Current user credentials
        """
        cls._LOG.debug("getting credentials based on JWT")
        response = requests.get("https://auth01.cmcc.it/realms/DDS")
        keycloak_public_key = f"""-----BEGIN PUBLIC KEY-----
    {response.json()['public_key']}
    -----END PUBLIC KEY-----"""
        if not authorization:
            cls._LOG.info(
                "`authorization` header is empty! using public profile"
            )
            return UserCredentials()
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
        cls._LOG.info("no user found for id `%s`", user_id)
        raise HTTPException(
            status_code=401,
            detail="User is not authorized!",
        )
