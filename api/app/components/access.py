from __future__ import annotations


import logging

from fastapi import HTTPException
from db.dbmanager.dbmanager import DBManager

from ..util import UserCredentials


class AccessManager:

    _LOG = logging.getLogger("AccessManager")

    @classmethod
    def assert_is_admin(cls, user_credentials: UserCredentials) -> bool:
        if DBManager().get_user_role_name(user_credentials.id) != "admin":
            raise HTTPException(
                status_code=401,
                detail=f"User `{user_credentials.id}` is not an admin!",
            )

    @classmethod
    def authenticate_user(cls, user_credentials: UserCredentials) -> bool:
        cls._LOG.debug(
            f"Authenticating the user with the user_id: {user_credentials.id}"
        )
        if user_credentials.is_public:
            cls._LOG.debug(f"Authentication successful. User is anonymouse!")
            return True
        user = DBManager().get_user_details(user_credentials.id)
        if user is None:
            cls._LOG.info(
                f"The user with id `{user_credentials.id}` does not exist!"
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    f"The user with id `{user_credentials.id}` does not exist!"
                ),
            )
        if user.api_key != user_credentials.key:
            cls._LOG.info(
                "Authentication failed! The key provided for the user_id"
                f" {user_credentials.id} was not valid!"
            )
            raise HTTPException(
                status_code=400,
                detail=f"The provided key is not valid.",
            )
        cls._LOG.debug(
            f"Authentication successful. User_id: {user_credentials.id}!"
        )

    @classmethod
    def is_user_eligible_for_role(
        cls,
        user_credentials: UserCredentials,
        product_role_name: None | str = "public",
    ) -> bool:
        cls._LOG.debug(
            "Verifying eligibility of the user_id:"
            f" {user_credentials.id} against role_name: {product_role_name}"
        )
        if product_role_name == "public":
            return True
        if user_credentials.is_public:
            return False
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        if user_role_name == "admin":
            return True
        elif user_role_name == product_role_name:
            return True
        else:
            return False

    @classmethod
    def is_user_eligible_for_request(
        cls, user_credentials: UserCredentials, request_id: int
    ) -> bool:
        cls._LOG.debug(
            "Verifying eligibility of the user_id:"
            f" {user_credentials.id} against request_id: {request_id}"
        )
        return True
        # NOTE: currently everyone is eligible for each download
        # request_details = DBManager().get_request_details(
        #     request_id=request_id
        # )
        # if (request_details is not None) and (
        #     str(request_details.user_id) == str(user_id)
        # ):
        #     return True
        # return False
