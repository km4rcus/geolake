"""Main module with dekube-dds API endpoints defined"""
__version__ = "2.0"
import os
from typing import Optional
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from aioprometheus import Counter, Summary, timer, MetricsMiddleware
from aioprometheus.asgi.starlette import metrics

from db.dbmanager.dbmanager import RequestStatus
from geoquery.geoquery import GeoQuery

from .components.access import AccessManager
from .components.dataset import DatasetManager
from .components.file import FileManager
from .components.request import RequestManager
from .util import UserCredentials
from .exceptions import MaximumAllowedSizeExceededError

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
    root_path=os.environ.get("ENDPOINT_PREFIX", "/api"),
)

# ======== CORS management ========= #
# TODO: origins should be limited!
ORIGINS = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======== Prometheus metrics management ========= #
app.add_middleware(MetricsMiddleware)
app.add_route("/metrics", metrics)

app.state.request_time = Summary(
    "request_processing_seconds", "Time spent processing request"
)
app.state.request = Counter("request_total", "Total number of requests")

# ======== Endpoints definitions ========= #
@app.get("/")
async def dds_info():
    """Return current version of the DDS API"""
    return f"DDS API {__version__}"


@app.get("/datasets")
@timer(app.state.request_time, labels={"route": "GET /datasets"})
async def get_datasets(
    user_token: Optional[str] = Header(None, convert_underscores=True)
):
    """List all products eligible for a user defined by user_token"""
    app.state.request.inc({"route": "GET /datasets"})
    user_credentials = UserCredentials(user_token)
    return DatasetManager.get_datasets_and_eligible_products_names(
        user_credentials=user_credentials
    )


@app.get("/datasets/{dataset_id}/{product_id}")
@timer(
    app.state.request_time,
    labels={"route": "GET /datasets/{dataset_id}/{product_id}"},
)
async def get_product_details(
    dataset_id: str,
    product_id: str,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Get details for the requested product if user is authorized"""
    app.state.request.inc({"route": "GET /datasets/{dataset_id}/{product_id}"})
    user_credentials = UserCredentials(user_token)
    return DatasetManager.get_details_for_product_if_eligible(
        user_credentials=user_credentials,
        dataset_id=dataset_id,
        product_id=product_id,
    )


@app.get("/datasets/{dataset_id}/{product_id}/metadata")
@timer(
    app.state.request_time,
    labels={"route": "GET /datasets/{dataset_id}/{product_id}/metadata"},
)
async def metadata(
    dataset_id: str,
    product_id: str,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Get metadata of the given product"""
    app.state.request.inc(
        {"route": "GET /datasets/{dataset_id}/{product_id}/metadata"}
    )
    user_credentials = UserCredentials(user_token)
    return DatasetManager.get_product_metadata(
        dataset_id=dataset_id,
        product_id=product_id,
        user_credentials=user_credentials,
    )


@app.post("/datasets/{dataset_id}/{product_id}/estimate")
@timer(
    app.state.request_time,
    labels={"route": "POST /datasets/{dataset_id}/{product_id}/estimate"},
)
async def estimate(
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    user_token: Optional[str] = Header(None, convert_underscores=True),
    unit: str = None,
):
    """Estimate the resulting size of the query"""
    app.state.request.inc(
        {"route": "POST /datasets/{dataset_id}/{product_id}/estimate"}
    )
    return DatasetManager.estimate(
        dataset_id=dataset_id, product_id=product_id, query=query, unit=unit
    )


@app.post("/datasets/{dataset_id}/{product_id}/execute")
@timer(
    app.state.request_time,
    labels={"route": "POST /datasets/{dataset_id}/{product_id}/execute"},
)
async def query(
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    format: Optional[str] = "netcdf",
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Schedule the job of data retrieve"""
    app.state.request.inc(
        {"route": "POST /datasets/{dataset_id}/{product_id}/execute"}
    )
    user_credentials = UserCredentials(user_token)
    try:
        DatasetManager.assert_estimated_size_below_product_limit(
            dataset_id=dataset_id,
            product_id=product_id,
            query=query,
            user_credentials=user_credentials,
        )
    except MaximumAllowedSizeExceededError as err:
        raise HTTPException(status_code=400, detail=str(err))
    return DatasetManager.retrieve_data_and_get_request_id(
        user_credentials=user_credentials,
        dataset_id=dataset_id,
        product_id=product_id,
        query=query,
        format=format,
    )


@app.get("/requests")
@timer(app.state.request_time, labels={"route": "GET /requests"})
async def get_requests(
    user_token: Optional[str] = Header(None, convert_underscores=True)
):
    """Get all requests for the user"""
    app.state.request.inc({"route": "GET /requests"})
    user_credentials = UserCredentials(user_token)
    AccessManager.assert_not_public(user_credentials)
    AccessManager.authenticate_user(user_credentials)
    return RequestManager.get_requests_details_for_user(
        user_credentials=user_credentials
    )


@app.get("/requests/{request_id}/status")
@timer(
    app.state.request_time,
    labels={"route": "GET /requests/{request_id}/status"},
)
async def get_request_status(request_id: int):
    """Get status of the request without authentication"""
    # NOTE: no auth required for checking status
    app.state.request.inc({"route": "GET /requests/{request_id}/status"})
    status, reason = RequestManager.get_request_status_for_request_id(
        request_id=request_id
    )
    if status is RequestStatus.FAILED:
        return {"status": status.name, "fail_reason": reason}
    return {"status": status.name}


@app.get("/requests/{request_id}/size")
@timer(
    app.state.request_time, labels={"route": "GET /requests/{request_id}/size"}
)
async def get_request_resulting_size(request_id: int):
    """Get size of the file being the result of the request"""
    app.state.request.inc({"route": "GET /requests/{request_id}/size"})
    return RequestManager.get_request_result_size(request_id=request_id)


@app.get("/download/{request_id}")
@timer(app.state.request_time, labels={"route": "GET /download/{request_id}"})
async def download_request_result(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Download result of the request"""
    app.state.request.inc({"route": "GET /download/{request_id}"})
    user_credentials = UserCredentials(user_token)
    # TODO: web portal need to pass User-Token header to this endpoint
    # AccessManager.assert_not_public(user_credentials)
    # AccessManager.authenticate_user(user_credentials)
    # if AccessManager.is_user_eligible_for_request(
    #     user_credentials=user_credentials, request_id=request_id
    # ):
    if True:
        path = FileManager.prepare_request_for_download_and_get_path(
            request_id=request_id
        )
        return FileResponse(path=path, filename=path)
    raise HTTPException(
        status_code=401,
        detail=(
            f"User with id: {user_credentials.id} is not authorized for"
            f" results of the request with id {request_id}"
        ),
    )


@app.get("/requests/{request_id}/uri")
@timer(
    app.state.request_time, labels={"route": "GET /requests/{request_id}/uri"}
)
async def get_request_uri(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Get download URI for the request"""
    app.state.request.inc({"route": "GET /requests/{request_id}/uri"})
    user_credentials = UserCredentials(user_token)
    AccessManager.authenticate_user(user_credentials)
    return RequestManager.get_request_uri_for_request_id(request_id=request_id)


@app.delete("/requests/{request_id}")
@timer(
    app.state.request_time, labels={"route": "DELETE /requests/{request_id}/"}
)
async def delete_request_uri(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Delete the request with 'request_id' from the database"""
    app.state.request.inc({"route": "DELETE /requests/{request_id}"})
    # TODO:
    raise HTTPException(status_code=400, detail="NotImplementedError")
