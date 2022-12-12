"""Module with tools for request management"""
from __future__ import annotations

import logging
from fastapi import HTTPException

from db.dbmanager.dbmanager import DBManager

from .meta import LoggableMeta
from ..utils.execution import log_execution_time
from ..exceptions import RequestNotFound
from ..context import Context


class RequestManager(metaclass=LoggableMeta):
    """Manager that handles requests stored in geokube-dds system"""

    _LOG = logging.getLogger("geokube.RequestManager")

    @classmethod
    @log_execution_time(_LOG)
    def get_request_result_size(cls, request_id: int) -> float:
        """Get size of the file being the result of the request with `request_id`

        Parameters
        ----------
        request_id : int
            ID of the request

        Returns
        -------
        size : int
            Size in bytes

        Raises
        -------
        RequestNotFound
            If the request was not found
        """
        if request := DBManager().get_request_details(request_id):
            return request.download.size_bytes
        cls._LOG.info("request with id '%s' could not be found", request_id)
        raise RequestNotFound

    @classmethod
    @log_execution_time(_LOG)
    def get_requests_details_for_user(
        cls, context: Context
    ) -> list[DBManager.Request]:
        """Get details of all requests for the user.

        Parameters
        ----------
        context : Context
            Context of the current http request

        Returns
        -------
        requests : list
            List of all requests done by the user
        """
        if context.user.is_public:
            cls._LOG.debug("attempt to get requests for anonymous user!")
        return DBManager().get_requests_for_user_id(user_id=context.user.id)

    @classmethod
    @log_execution_time(_LOG)
    def get_request_status_for_request_id(
        cls, request_id: int
    ) -> tuple[str, str]:
        """Get request status and the reason of the eventual fail.
        The second item is `None`, it status is other than failed.

        Parameters
        ----------
        request_id : int
            ID of the request

        Returns
        -------
        status : tuple
            Tuple of status and, eventually, fail reason.
        """
        try:
            status, reason = DBManager().get_request_status_and_reason(
                request_id
            )
        except IndexError as err:
            cls._LOG.error("request with id: '%s' was not found!", request_id)
            raise RequestNotFound from err
        return status, reason

    @classmethod
    @log_execution_time(_LOG)
    def get_request_uri_for_request_id(cls, request_id) -> str:
        """Get URI for the request.

        Parameters
        ----------
        request_id : int
            ID of the request

        Returns
        -------
        uri : str
            URI for the download associated with the given request

        Raises
        -------
        HTTPException
            400 if the request does not generated the file
        """
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
