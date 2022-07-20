from __future__ import annotations

import os
import logging
from zipfile import ZipFile

from fastapi import HTTPException

from db.dbmanager.dbmanager import DBManager, RequestStatus


class FileManager:

    _LOG = logging.getLogger("FileManager")

    @classmethod
    def prepare_request_for_download_and_get_path(cls, request_id: str | int):
        cls._LOG.debug(f"Preparing downloads for request id: {request_id}...")
        db = DBManager()
        (
            request_status,
            fail_reason,
        ) = DBManager().get_request_status_and_reason(request_id=request_id)
        if request_status is not RequestStatus.DONE:
            cls._LOG.debug(
                f"Request with id: {request_id} does not exist or it is not"
                " finished yet!"
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
                f"File {download_details.location_path} does not exists!"
            )
            raise HTTPException(status_code=404, detail="File was not found!")
        return download_details.location_path
