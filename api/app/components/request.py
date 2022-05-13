from __future__ import annotations

import logging
from fastapi import HTTPException
from geoquery.geoquery import GeoQuery
from db.dbmanager.dbmanager import DBManager

from .access import AccessManager
from ..util import UserCredentials


class RequestManager:

    _LOG = logging.getLogger("RequestManager")

    @classmethod
    def get_requests_details_for_user(
        cls, user_credentials: UserCredentials
    ) -> list[DBManager.Request]:
        if user_credentials.is_public:
            raise HTTPException(
                status_code=401, detail="Anonymous user doesn't have requests!"
            )
        return DBManager().get_requests_for_user_id(
            user_id=user_credentials.id
        )

    @classmethod
    def get_request_status_for_request_id(
        cls, request_id: int
    ) -> DBManager.Request:
        status = DBManager().get_request_status(request_id)
        if status is None:
            raise HTTPException(
                status_code=400,
                detail=f"Request with id: {request_id} does not exist!",
            )
        return status

    @classmethod
    def get_request_uri_for_request_id(cls, request_id) -> str:
        download_details = DBManager().get_download_details_for_request_id(
            request_id
        )
        return download_details.download_uri
