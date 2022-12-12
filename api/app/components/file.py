"""Module with tools for files management"""
import os
import logging
from zipfile import ZipFile

from fastapi import HTTPException

from db.dbmanager.dbmanager import DBManager, RequestStatus

from .meta import LoggableMeta
from ..decorators import log_execution_time
from ..exceptions import RequestNotYetAccomplished
from ..context import Context
from ..decorators import authenticate


class FileManager(metaclass=LoggableMeta):
    """Manager class to handle files produced by geokube-dds"""

    _LOG = logging.getLogger("geokube.FileManager")

    @classmethod
    @log_execution_time(_LOG)
    @authenticate(enable_public=False)
    def prepare_request_for_download_and_get_path(
        cls, context: Context, request_id: int
    ):
        """Realize the logic for the endpoint:

        `GET /download/{request_id}`

        Get location path of the file being the result of
        the request with `request_id`.

        Parameters
        ----------
        request_id : int
            ID of the request

        Returns
        -------
        path : str
            The location of the resulting file

        Raises
        -------
        RequestNotYetAccomplished
            If dds request was not finished
        FileNotFoundError
            If file was not found
        """
        cls._LOG.debug("preparing downloads for request id: %s", request_id)
        (
            request_status,
            _,
        ) = DBManager().get_request_status_and_reason(request_id=request_id)
        if request_status is not RequestStatus.DONE:
            cls._LOG.debug(
                "request with id: '%s' does not exist or it is not finished"
                " yet!",
                request_id,
            )
            raise RequestNotYetAccomplished
        download_details = DBManager().get_download_details_for_request(
            request_id=request_id
        )
        if not os.path.exists(download_details.location_path):
            cls._LOG.error(
                "file '%s' does not exists!", download_details.location_path
            )
            raise FileNotFoundError
        return download_details.location_path
