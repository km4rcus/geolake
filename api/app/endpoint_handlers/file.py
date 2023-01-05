"""Module with functions to handle file related endpoints"""
import os

from fastapi.responses import FileResponse
from db.dbmanager.dbmanager import DBManager, RequestStatus

from ..auth import Context
from ..api_logging import get_dds_logger
from ..metrics import log_execution_time
from .. import exceptions as exc

log = get_dds_logger(__name__)


@log_execution_time(log)
def download_request_result(context: Context, request_id: int):
    """Realize the logic for the endpoint:

    `GET /download/{request_id}`

    Get location path of the file being the result of
    the request with `request_id`.

    Parameters
    ----------
    context : Context
        Context of the current http request
    request_id : int
        ID of the request

    Returns
    -------
    path : str
        The location of the resulting file

    Raises
    -------
    RequestNotYetAccomplished
        If dds request was not yet finished
    FileNotFoundError
        If file was not found
    """
    log.debug(
        "preparing downloads for request id: %s",
        request_id,
        extra={"rid": context.rid},
    )
    (
        request_status,
        _,
    ) = DBManager().get_request_status_and_reason(request_id=request_id)
    if request_status is not RequestStatus.DONE:
        log.debug(
            "request with id: '%s' does not exist or it is not finished yet!",
            request_id,
            extra={"rid": context.rid},
        )
        raise exc.RequestNotYetAccomplished(request_id=request_id)
    download_details = DBManager().get_download_details_for_request(
        request_id=request_id
    )
    if not os.path.exists(download_details.location_path):
        log.error(
            "file '%s' does not exists!",
            download_details.location_path,
            extra={"rid": context.rid},
        )
        raise FileNotFoundError
    return FileResponse(
        path=download_details.location_path,
        filename=download_details.location_path.split(os.sep)[-1],
    )
