"""Module responsible for authnetication and authorization functionalities"""
from __future__ import annotations

import logging
import requests
import jwt

from db.dbmanager.dbmanager import DBManager

from .utils import UserCredentials, log_execution_time
from .meta import LoggableMeta
from .exceptions import AuthenticationFailed, UserAlreadyExistError


class AccessManager(metaclass=LoggableMeta):
    """The component for managing access to data, authentication, and
    authorization of a user"""

    _LOG = logging.getLogger("geokube.AccessManager")

    @classmethod
    def _decode_jwt(cls, authorization: str) -> dict:
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
        return jwt.decode(token, keycloak_public_key, audience="account")

    @classmethod
    def _infer_roles(cls, user: dict) -> list[str]:
        if user["email"].endswith("cmcc.it"):
            return ["cmcc"]
        return ["public"]

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
        user_id = cls._decode_jwt(authorization=authorization)["sub"]
        # NOTE: if user with `user_id` is defined with DB,
        # we claim authorization was successful
        if user_details := DBManager().get_user_details(user_id):
            return UserCredentials(
                user_id=user_id, user_token=user_details.api_key
            )
        cls._LOG.info("no user found for id `%s`", user_id)
        raise AuthenticationFailed

    @classmethod
    @log_execution_time(_LOG)
    def add_user(cls, authorization: str):
        """Add user to the database and return generated api key

        Parameters
        ----------
        authorization : str
            `Authorization` token

        Returns
        -------
        user : User
            User added to DB

        Raises
        ------
        UserAlreadyExistError
            Raised if user is already present in the database
        """
        user = cls._decode_jwt(authorization=authorization)
        if (user_details := DBManager().get_user_details(user["sub"])) is None:
            contact_name = " ".join([user["given_name"], user["family_name"]])
            roles = cls._infer_roles(user_details)
            return DBManager().add_user(
                contact_name=contact_name,
                user_id=user["sub"],
                roles_names=roles,
            )
        raise UserAlreadyExistError
