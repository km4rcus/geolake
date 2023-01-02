"""Endpoints for `web` component"""
__version__ = "2.0"
import os
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from geoquery.geoquery import GeoQuery

from aioprometheus import Counter, Summary, timer, MetricsMiddleware
from aioprometheus.asgi.starlette import metrics

from .access import AccessManager
from .models import ListOfDatasets, ListOfRequests
from .requester import GeokubeAPIRequester
from .widget import WidgetFactory
from .exceptions import (
    AuthenticationFailed,
    GeokubeAPIRequestFailed,
)
from .context import Context
from .utils.numeric import prepare_estimate_size_message

app = FastAPI(
    title="geokube-dds API for Webportal",
    description="REST API for DDS Webportal",
    version=__version__,
    contact={
        "name": "geokube Contributors",
        "email": "geokube@googlegroups.com",
    },
    license_info={
        "name": "Apache 2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },
    root_path=os.environ.get("ENDPOINT_PREFIX", "/web"),
    on_startup=[GeokubeAPIRequester.init],
)

# ======== CORS ========= #
# TODO: origins should be limited!
ORIGINS = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======== Prometheus metrics ========= #
app.add_middleware(MetricsMiddleware)
app.add_route("/metrics", metrics)

app.state.request_time = Summary(
    "request_processing_seconds", "Time spent processing request"
)
app.state.request = Counter("request_total", "Total number of requests")

# ======== Endpoints definitions ========= #
@app.get("/")
async def dds_info():
    """Return current version of the DDS API for the Webportal"""
    return f"DDS Webportal API {__version__}"


@app.get("/datasets")
@timer(app.state.request_time, labels={"route": "GET /datasets"})
async def get_datasets(
    request: Request,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get list of eligible datasets for the home page of the Webportal"""
    app.state.request.inc({"route": "GET /datasets"})
    try:
        context = Context(request, authorization, enable_public=True)
        datasets = GeokubeAPIRequester.get(url="/datasets", context=context)
    except GeokubeAPIRequestFailed as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    else:
        return ListOfDatasets.from_details(datasets)


@app.get("/datasets/{dataset_id}/{product_id}")
@timer(
    app.state.request_time,
    labels={"route": "GET /datasets/{dataset_id}/{product_id}"},
)
async def get_details_product(
    request: Request,
    dataset_id: str,
    product_id: str,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get details for Webportal"""
    app.state.request.inc({"route": "GET /datasets/{dataset_id}/{product_id}"})
    try:
        context = Context(request, authorization, enable_public=True)
        details = GeokubeAPIRequester.get(
            url=f"/datasets/{dataset_id}/{product_id}", context=context
        )
    except GeokubeAPIRequestFailed as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    else:
        return WidgetFactory(details).widgets


@app.post("/datasets/{dataset_id}/{product_id}/execute")
@timer(
    app.state.request_time,
    labels={"route": "POST /datasets/{dataset_id}/{product_id}/execute"},
)
async def execute(
    request: Request,
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Schedule the job of data retrieving by using geokube-dds API"""
    app.state.request.inc(
        {"route": "POST /datasets/{dataset_id}/{product_id}/execute"}
    )
    try:
        context = Context(request, authorization, enable_public=False)
        response = GeokubeAPIRequester.post(
            url=f"/datasets/{dataset_id}/{product_id}/execute",
            data=query.json(),
            context=context,
        )
    except AuthenticationFailed as err:
        raise HTTPException(
            status_code=401, detail="User could not be authenticated"
        ) from err
    except GeokubeAPIRequestFailed as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    else:
        return response


@app.post("/datasets/{dataset_id}/{product_id}/estimate")
@timer(
    app.state.request_time,
    labels={"route": "POST /datasets/{dataset_id}/{product_id}/estimate"},
)
async def estimate(
    request: Request,
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Estimate the resulting size of the query by using geokube-dds API"""
    app.state.request.inc(
        {"route": "POST /datasets/{dataset_id}/{product_id}/estimate"}
    )
    try:
        context = Context(request, authorization, enable_public=True)
        response = GeokubeAPIRequester.post(
            url=f"/datasets/{dataset_id}/{product_id}/estimate?unit=GB",
            data=query.json(),
            context=context,
        )
        metadata = GeokubeAPIRequester.get(
            url=f"/datasets/{dataset_id}/{product_id}/metadata",
            context=context,
        )
    except GeokubeAPIRequestFailed as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    else:
        return prepare_estimate_size_message(
            maximum_allowed_size_gb=metadata.get("maximum_query_size_gb", 10),
            estimated_size_gb=response.get("value"),
        )


# TODO: !!!access should be restricted!!!
@app.get("/get_api_key")
async def get_api_key(
    request: Request,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get API key for a user with the given `Authorization` token.
    Adds user to DB and generates api key, if user is not found."""
    try:
        context = Context(request, authorization, enable_public=False)
    except AuthenticationFailed:
        AccessManager.add_user(authorization=authorization)
        context = Context(request, authorization, enable_public=False)
    else:
        return {"key": f"{context.user.id}:{context.user.key}"}


@app.get("/requests")
@timer(app.state.request_time, labels={"route": "GET /requests"})
async def get_requests(
    request: Request,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get requests for a user the given Authorization token"""
    app.state.request.inc({"route": "GET /requests"})
    try:
        context = Context(request, authorization, enable_public=False)
        response_json = GeokubeAPIRequester.get(
            url="/requests", context=context
        )
    except AuthenticationFailed as err:
        raise HTTPException(
            status_code=401, detail="User could not be authenticated"
        ) from err
    except GeokubeAPIRequestFailed as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    else:
        requests = ListOfRequests(data=response_json)
        requests.add_requests_url_prefix(
            os.environ.get("DOWNLOAD_PREFIX", GeokubeAPIRequester.API_URL)
        )
        return requests
