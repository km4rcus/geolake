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
            cls._LOG.debug("Attempt to get requests for anonymous user!")
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
        try:
            status, reason = DBManager().get_request_status_and_reason(
                request_id
            )
        except IndexError as e:
            cls._LOG.error(f"Request with id: `{request_id}` was not found!")
            raise HTTPException(
                status_code=400,
                detail=f"Request with id: {request_id} does not exist!",
            )
        return status, reason

    @classmethod
    def get_request_uri_for_request_id(cls, request_id) -> str:
        try:
            download_details = DBManager().get_download_details_for_request_id(
                request_id
            )
        except IndexError as e:
            cls._LOG.error(f"Request with id: `{request_id}` was not found!")
            raise HTTPException(
                status_code=400,
                detail=f"Request with id: `{request_id}` does not exist",
            )
        if download_details is None:
            (
                request_status,
                fail_reason,
            ) = DBManager().get_request_status_and_reason(request_id)
            cls._LOG.info(
                f"Download URI not found for request id: `{request_id}`."
                f" Request status is `{request_status}`"
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Request with id: `{request_id}` does not have download"
                    f" URI. It has status: `{request_status}`!"
                ),
            )
        return download_details.download_uri
