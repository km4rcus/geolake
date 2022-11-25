"""Module with tools for access management"""
from __future__ import annotations


import logging

from fastapi import HTTPException
from db.dbmanager.dbmanager import DBManager

from ..util import UserCredentials, log_execution_time
from .meta import LoggableMeta


class AccessManager(metaclass=LoggableMeta):
    """Manager that handles access to the geokube-dds"""

    _LOG = logging.getLogger("geokube.AccessManager")

    @classmethod
    def assert_is_admin(cls, user_credentials: UserCredentials) -> bool:
        """Assert that user has an 'admin' role

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user

        Raises
        -------
        HTTPException
            401 if user is not an admin
        """
        if DBManager().get_user_role_name(user_credentials.id) != "admin":
            raise HTTPException(
                status_code=401,
                detail=f"User `{user_credentials.id}` is not an admin!",
            )

    @classmethod
    def assert_not_public(cls, user_credentials: UserCredentials) -> bool:
        """Assert that user is authenticated

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user

        Raises
        -------
        HTTPException
            401 if user is public
        """
        if user_credentials.is_public:
            raise HTTPException(
                status_code=401,
                detail="You need to authenticate!",
            )

    @classmethod
    @log_execution_time(_LOG)
    def authenticate_user(cls, user_credentials: UserCredentials):
        """Authenticate user given the credentials.

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user

        Raises
        -------
        HTTPException
            400 if user does not exist or the key is not valid

        """
        cls._LOG.debug(
            "authenticating the user with the user_id: %s", user_credentials.id
        )
        if user_credentials.is_public:
            cls._LOG.debug("user is anonymouse!")
        user = DBManager().get_user_details(user_credentials.id)
        if user is None:
            cls._LOG.info(
                "user with id '%s' does not exist!", user_credentials.id
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    f"The user with id `{user_credentials.id}` does not exist!"
                ),
            )
        if user.api_key != user_credentials.key:
            cls._LOG.info(
                "authentication failed! The key provided for the user_id '%s'"
                " was not valid!",
                user_credentials.id,
            )
            raise HTTPException(
                status_code=400,
                detail="The provided key is not valid.",
            )
        cls._LOG.debug(
            "authentication successful. User_id '%s'!", user_credentials.id
        )

    @classmethod
    @log_execution_time(_LOG)
    def is_user_eligible_for_role(
        cls,
        user_credentials: UserCredentials,
        product_role_name: None | str = "public",
    ) -> bool:
        """Check if user is eligible for the given product's role.
        If no product role name is defined, it's treated as the 'public'
        profile.

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user
        product_role_name : str, optional, default="public"
            The name of the product's role

        Returns
        -------
        is_eligible : bool
            `True` if user is eligible, `False` otherwise
        """
        cls._LOG.debug(
            "verifying eligibility of the user_id '%s' against role_name:"
            " '%s'",
            user_credentials.id,
            product_role_name,
        )
        if product_role_name is None or product_role_name == "public":
            return True
        if user_credentials.is_public:
            return False
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        if user_role_name == "admin":
            return True
        if user_role_name == product_role_name:
            return True
        return False

    @classmethod
    @log_execution_time(_LOG)
    def is_user_eligible_for_request(
        cls, user_credentials: UserCredentials, request_id: int
    ) -> bool:
        """Check if user is eligible to see request's details

        Parameters
        ----------
        user_credentials : UserCredentials
            The credentials of the current user
        request_id : int, optional, default="public"
            ID of the request to check

        Returns
        -------
        is_eligible : bool
            `True` if user is eligible, `False` otherwise
        """
        cls._LOG.debug(
            "verifying eligibility of the user_id: '%s' against request_id:"
            " '%s'",
            user_credentials.id,
            request_id,
        )
        request_details = DBManager().get_request_details(
            request_id=request_id
        )
        if (request_details is not None) and (
            str(request_details.user_id) == str(user_credentials.id)
        ):
            return True
        return False
