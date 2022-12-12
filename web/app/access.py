"""Module responsible for authnetication and authorization functionalities"""
from __future__ import annotations

import logging
import requests
import jwt

from db.dbmanager.dbmanager import DBManager

from .utils import UserCredentials, log_execution_time
from .meta import LoggableMeta
from .exceptions import AuthenticationFailed


class AccessManager(metaclass=LoggableMeta):
    """The component for managing access to data, authentication, and
    authorization of a user"""

    _LOG = logging.getLogger("geokube.AccessManager")

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

        Raises
        -------
        AuthenticationFailed
            If user was not authenticated properly
        """
        cls._LOG.debug("getting credentials based on JWT")
        response = requests.get(
            "https://auth01.cmcc.it/realms/DDS", timeout=10
        )
        # NOTE: public key 2nd and 3rd lines cannot be indented
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
        raise AuthenticationFailed
