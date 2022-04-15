from __future__ import annotations


import logging

from fastapi import HTTPException
from db.dbmanager.dbmanager import DBManager


class AccessManager:

    _LOG = logging.getLogger("AccessManager")

    @classmethod
    def authorize_and_return_user(
        cls, user_id: str | int, api_key: str
    ) -> int | None:
        db = DBManager()
        cls._LOG.debug(f"Authorizing user with id: {user_id}")
        user_details = db.get_user_details(user_id)
        if user_details is None:
            cls._LOG.debug(f"User with id: {user_id} does not exist!")
            raise HTTPException(
                status_code=400,
                detail=f"User with id: {user_id} does not exist!",
            )
        if user_details.api_key != api_key:
            cls._LOG.debug(
                f"Incorrect password for the user with id: {user_id}!"
            )
            raise HTTPException(
                status_code=400,
                detail=f"Incorrect api key for the user with id: {user_id}!",
            )
        return user_details

    @classmethod
    def get_role_details(cls, role_id: str | int):
        return DBManager().get_role_details(role_id)

    @classmethod
    def is_user_role_eligible(
        cls,
        user_role_name: str | None = "public",
        product_role_name: str | None = "public",
    ) -> bool:
        return DBManager().has_sufficient_privileges(
            role_name=user_role_name, reference_role_name=product_role_name,
        )
