__version__ = "2.0"
import os
from typing import Optional
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from enum import Enum
from pydantic import BaseModel

from db.dbmanager.dbmanager import DBManager, RequestStatus
from geoquery.geoquery import GeoQuery

from .components.access import AccessManager
from .components.dataset import DatasetManager
from .components.file import FileManager
from .components.request import RequestManager
from .util import UserCredentials

_pref = os.environ.get("ENDPOINT_PREFIX", "/api")
app = FastAPI(
    title="geokube-dds API",
    description="REST API for geokube-dds",
    version=__version__,
    contact={
        "name": "geokube Contributors",
        "email": "geokube@googlegroups.com",
    },
    license_info={
        "name": "Apache 2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },
    docs_url=f"{_pref}/docs",
    openapi_url=f"{_pref}/openapi.json",
)
app.router.prefix = _pref


@app.get("/")
async def dds_info():
    """Return current version of the DDS API"""
    return f"DDS API {__version__}"


@app.get("/datasets")
async def datasets(
    user_token: Optional[str] = Header(None, convert_underscores=True)
):
    """List all products eligible for a user defined by user_token"""
    user_credentials = UserCredentials(user_token)
    return DatasetManager.get_eligible_products_for_all_datasets(
        user_credentials=user_credentials
    )


@app.get("/datasets/{dataset_id}")
async def dataset(
    dataset_id: str,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Get eligible products for the given dataset"""
    user_credentials = UserCredentials(user_token)
    return DatasetManager.get_eligible_products_for_dataset(
        user_credentials=user_credentials, dataset_id=dataset_id
    )


@app.get("/datasets/{dataset_id}/{product_id}")
async def dataset(
    dataset_id: str,
    product_id: str,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Get details for the requested product if user is authorized"""
    user_credentials = UserCredentials(user_token)
    return DatasetManager.get_details_if_product_eligible(
        user_credentials=user_credentials,
        dataset_id=dataset_id,
        product_id=product_id,
    )


@app.post("/datasets/{dataset_id}/{product_id}/estimate")
async def estimate(
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Estimate the resulting size of the query"""
    return DatasetManager.estimate(
        dataset_id=dataset_id, product_id=product_id, query=query
    )


@app.post("/datasets/{dataset_id}/{product_id}/execute")
async def query(
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    format: Optional[str] = "netcdf",
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Schedule the job of data retrieving"""
    # TODO: Validation Query Schema
    # TODO: estimate the size and will not execute if it is above the limit
    user_credentials = UserCredentials(user_token)
    return DatasetManager.retrieve_data_and_get_request_id(
        user_credentials=user_credentials,
        dataset_id=dataset_id,
        product_id=product_id,
        query=query,
        format=format,
    )


@app.get("/requests")
async def get_requests(
    user_token: Optional[str] = Header(None, convert_underscores=True)
):
    """Get all requests for the user"""
    user_credentials = UserCredentials(user_token)
    AccessManager.authenticate_user(user_credentials)
    return RequestManager.get_requests_details_for_user(
        user_credentials=user_credentials
    )


@app.get("/requests/{request_id}/status")
async def get_request_status(request_id: int):
    """Get status of the request without authentication"""
    # NOTE: no auth required for checking status
    status, reason = RequestManager.get_request_status_for_request_id(
        request_id=request_id
    )
    if status is RequestStatus.FAILED:
        return {"status": status.name, "fail_reason": reason}
    else:
        return {"status": status.name}


@app.get("/download/{request_id}")
async def download_request_result(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Download result of the request"""
    user_credentials = UserCredentials(user_token)
    AccessManager.authenticate_user(user_credentials)
    if AccessManager.is_user_eligible_for_request(
        user_credentials=user_credentials, request_id=request_id
    ):
        path = FileManager.prepare_request_for_download_and_get_path(
            request_id=request_id
        )
        return FileResponse(path=path, filename=path)
    else:
        raise HTTPException(
            status_code=401,
            detail=(
                f"User with id: {user_credentials.id} is not authorized for"
                f" results of the request with id {request_id}"
            ),
        )


@app.get("/requests/{request_id}/uri")
async def get_request_uri(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Get download URI for the request"""
    user_credentials = UserCredentials(user_token)
    AccessManager.authenticate_user(user_credentials)
    return RequestManager.get_request_uri_for_request_id(request_id=request_id)


@app.delete("/requests/{request_id}/")
async def get_request_uri(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    # TODO:
    raise HTTPException(status_code=400, detail="NotImplementedError")
