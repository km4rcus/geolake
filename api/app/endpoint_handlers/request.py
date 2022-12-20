"""Modules with functions realizing logic for requests-related endpoints"""
from db.dbmanager.dbmanager import DBManager

from ..auth import Context, assert_not_anonymous
from ..logging import get_dds_logger
from .. import exceptions as exc
from ..metrics import log_execution_time

log = get_dds_logger(__name__)


@log_execution_time(log)
@assert_not_anonymous
def get_requests(context: Context):
    """Realize the logic for the endpoint:

    `GET /requests`

    Get details of all requests for the user.

    Parameters
    ----------
    context : Context
        Context of the current http request

    Returns
    -------
    requests : list
        List of all requests done by the user
    """
    return DBManager().get_requests_for_user_id(user_id=context.user.id)


@log_execution_time(log)
def get_request_status(context: Context, request_id: int):
    """Realize the logic for the endpoint:

    `GET /requests/{request_id}/status`

    Get request status and the reason of the eventual fail.
    The second item is `None`, it status is other than failed.

    Parameters
    ----------
    context : Context
        Context of the current http request
    request_id : int
        ID of the request

    Returns
    -------
    status : tuple
        Tuple of status and fail reason.
    """
    try:
        status, reason = DBManager().get_request_status_and_reason(request_id)
    except IndexError as err:
        log.error(
            "request with id: '%s' was not found!",
            request_id,
            extra={"rid": context.rid},
        )
        raise exc.RequestNotFound(request_id=request_id) from err
    return {"status": status.name, "fail_reason": reason}


@log_execution_time(log)
def get_request_resulting_size(context: Context, request_id: int):
    """Realize the logic for the endpoint:

    `GET /requests/{request_id}/size`

    Get size of the file being the result of the request with `request_id`

    Parameters
    ----------
    context : Context
        Context of the current http request
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
    log.info(
        "request with id '%s' could not be found",
        request_id,
        extra={"rid": context.rid},
    )
    raise exc.RequestNotFound(request_id=request_id)


@log_execution_time(log)
def get_request_uri(context: Context, request_id: int):
    """
    Realize the logic for the endpoint:

    `GET /requests/{request_id}/uri`

    Get URI for the request.

    Parameters
    ----------
    request_id : int
        ID of the request

    Returns
    -------
    uri : str
        URI for the download associated with the given request
    """
    try:
        download_details = DBManager().get_download_details_for_request_id(
            request_id
        )
    except IndexError as err:
        log.error(
            "request with id: '%s' was not found!",
            request_id,
            extra={"rid": context.rid},
        )
        raise exc.RequestNotFound(request_id=request_id) from err
    if download_details is None:
        (
            request_status,
            _,
        ) = DBManager().get_request_status_and_reason(request_id)
        log.info(
            "download URI not found for request id: '%s'."
            " Request status is '%s'",
            request_id,
            request_status,
            extra={"rid": context.rid},
        )
        raise exc.RequestStatusNotDone(
            request_id=request_id, request_status=request_status
        )
    return download_details.download_uri
