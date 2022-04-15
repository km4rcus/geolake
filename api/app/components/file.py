from __future__ import annotations

import os
import logging

from fastapi import HTTPException

from db.dbmanager.dbmanager import DBManager, RequestStatus


class FileManager:

    _LOG = logging.getLogger("FileManager")

    @classmethod
    def prepare_for_download_and_get_path(cls, request_id: str | int):
        # TODO: eventually zip
        db = DBManager()
        request_status = DBManager().get_request_status(request_id=request_id)
        if request_status is not RequestStatus.DONE:
            raise HTTPException(
                status_code=404,
                details=f"Request with id: {request_id} is not finished!",
            )
        download_details = db.get_download_details_for_request(
            request_id=request_id
        )
        return os.path.join(
            download_details.location_path,
            os.listdir(download_details.location_path)[0],
        )
