import logging
from typing import Optional

from fastapi import HTTPException
from db.dbmanager.dbmanager import DBManager


class AccessManager:

    _LOG = logging.getLogger("AccessManager")

    @classmethod
    def authorize_user_and_get_role(cls, user_id: str, api_key: str) -> Optional[int]:
        db = DBManager()
        cls._LOG.debug(f"Authorizing user with id: {user_id}")
        user_details = db.get_user_details(user_id)
        if user_details is None:
            cls._LOG.debug(f"User with id: {user_id} does not exist!")
            raise HTTPException(status_code=400, detail=f"User with id: {user_id} does not exist!")
        if user_details.api_key != api_key:
            cls._LOG.debug(f"Incorrect password for the user with id: {user_id}!")
            raise HTTPException(status_code=400, detail=f"Incorrect api key for the user with id: {user_id}!") 
        return db.get_role_details(user_details.role_id)



