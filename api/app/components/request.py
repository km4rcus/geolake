from __future__ import annotations

import logging
from fastapi import HTTPException
from geoquery.geoquery import GeoQuery
from db.dbmanager.dbmanager import DBManager

from .access import AccessManager
from .meta import LoggableMeta
from ..util import UserCredentials, log_execution_time


class RequestManager(metaclass=LoggableMeta):

    _LOG = logging.getLogger("RequestManager")

    @classmethod
    @log_execution_time(_LOG)
    def get_requests_details_for_user(
        cls, user_credentials: UserCredentials
    ) -> list[DBManager.Request]:
        if user_credentials.is_public:
            cls._LOG.debug("attempt to get requests for anonymous user!")
            raise HTTPException(
                status_code=401, detail="Anonymous user doesn't have requests!"
            )
        return DBManager().get_requests_for_user_id(
            user_id=user_credentials.id
        )

    @classmethod
    @log_execution_time(_LOG)
    def get_request_status_for_request_id(
        cls, request_id: int
    ) -> DBManager.Request:
        try:
            status, reason = DBManager().get_request_status_and_reason(
                request_id
            )
        except IndexError as err:
            cls._LOG.error("request with id: '%s' was not found!", request_id)
            raise HTTPException(
                status_code=400,
                detail=f"Request with id: {request_id} does not exist!",
            ) from err
        return status, reason

    @classmethod
    @log_execution_time(_LOG)
    def get_request_uri_for_request_id(cls, request_id) -> str:
        try:
            download_details = DBManager().get_download_details_for_request_id(
                request_id
            )
        except IndexError as err:
            cls._LOG.error("request with id: '%s' was not found!", request_id)
            raise HTTPException(
                status_code=400,
                detail=f"Request with id: `{request_id}` does not exist",
            ) from err
        if download_details is None:
            (
                request_status,
                _,
            ) = DBManager().get_request_status_and_reason(request_id)
            cls._LOG.info(
                "download URI not found for request id: '%s'."
                " Request status is '%s'",
                request_id,
                request_status,
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Request with id: `{request_id}` does not have download"
                    f" URI. It has status: `{request_status}`!"
                ),
            )
        return download_details.download_uri
