from __future__ import annotations

import os
import logging
from zipfile import ZipFile

from fastapi import HTTPException

from db.dbmanager.dbmanager import DBManager, RequestStatus

from .meta import LoggableMeta
from ..util import log_execution_time


class FileManager(metaclass=LoggableMeta):

    _LOG = logging.getLogger("FileManager")

    @classmethod
    @log_execution_time(_LOG)
    def prepare_request_for_download_and_get_path(cls, request_id: str | int):
        cls._LOG.debug(f"Preparing downloads for request id: {request_id}...")
        db = DBManager()
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
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Request with id: {request_id} does not exist or it is"
                    " not finished yet!"
                ),
            )
        download_details = db.get_download_details_for_request(
            request_id=request_id
        )
        if not os.path.exists(download_details.location_path):
            cls._LOG.error(
                "file '%s' does not exists!", download_details.location_path
            )
            raise HTTPException(status_code=404, detail="File was not found!")
        return download_details.location_path
