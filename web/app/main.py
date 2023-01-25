"""Endpoints for `web` component"""
__version__ = "2.0"
import os
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from geoquery.geoquery import GeoQuery

from aioprometheus import (
    Counter,
    Summary,
    Gauge,
    timer,
    inprogress,
    count_exceptions,
    MetricsMiddleware,
)
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
if "ALLOWED_CORS_ORIGINS_REGEX" in os.environ:
    cors_kwargs = {
        "allow_origin_regex": os.environ["ALLOWED_CORS_ORIGINS_REGEX"]
    }
else:
    cors_kwargs = {"allow_origins": ["*"]}

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    **cors_kwargs,
)

# ======== Prometheus metrics ========= #
app.add_middleware(MetricsMiddleware)
app.add_route("/metrics", metrics)

app.state.web_request_duration_seconds = Summary(
    "web_request_duration_seconds", "Request duration"
)
app.state.web_http_requests_total = Counter(
    "web_http_requests_total", "Total number of requests"
)
app.state.web_exceptions_total = Counter(
    "web_exceptions_total", "Total number of exception raised"
)
app.state.web_requests_inprogress_total = Gauge(
    "web_requests_inprogress_total", "Endpoints being currently in progress"
)

# ======== Endpoints definitions ========= #
@app.get("/")
async def dds_info():
    """Return current version of the DDS API for the Webportal"""
    return f"DDS Webportal API {__version__}"


@app.get("/datasets")
@timer(
    app.state.web_request_duration_seconds, labels={"route": "GET /datasets"}
)
@count_exceptions(
    app.state.web_exceptions_total, labels={"route": "GET /datasets"}
)
@inprogress(
    app.state.web_requests_inprogress_total, labels={"route": "GET /datasets"}
)
async def get_datasets(
    request: Request,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get list of eligible datasets for the home page of the Webportal"""
    app.state.web_http_requests_total.inc({"type": "GET /datasets"})
    try:
        context = Context(request, authorization, enable_public=True)
        datasets = GeokubeAPIRequester.get(url="/datasets", context=context)
    except GeokubeAPIRequestFailed as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    else:
        return ListOfDatasets.from_details(datasets)


@app.get("/datasets/{dataset_id}/{product_id}")
@timer(
    app.state.web_request_duration_seconds,
    labels={"route": "GET /datasets/{dataset_id}/{product_id}"},
)
@count_exceptions(
    app.state.web_exceptions_total,
    labels={"route": "GET /datasets/{dataset_id}/{product_id}"},
)
@inprogress(
    app.state.web_requests_inprogress_total,
    labels={"route": "GET /datasets/{dataset_id}/{product_id}"},
)
async def get_details_product(
    request: Request,
    dataset_id: str,
    product_id: str,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get details for Webportal"""
    app.state.web_http_requests_total.inc(
        {"type": "GET /datasets/{dataset_id}/{product_id}"}
    )
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
    app.state.web_request_duration_seconds,
    labels={"route": "POST /datasets/{dataset_id}/{product_id}/execute"},
)
@count_exceptions(
    app.state.web_exceptions_total,
    labels={"route": "POST /datasets/{dataset_id}/{product_id}/execute"},
)
@inprogress(
    app.state.web_requests_inprogress_total,
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
    app.state.web_http_requests_total.inc(
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
    app.state.web_request_duration_seconds,
    labels={"route": "POST /datasets/{dataset_id}/{product_id}/estimate"},
)
@count_exceptions(
    app.state.web_exceptions_total,
    labels={"route": "POST /datasets/{dataset_id}/{product_id}/estimate"},
)
@inprogress(
    app.state.web_requests_inprogress_total,
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
    app.state.web_http_requests_total.inc(
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
@timer(
    app.state.web_request_duration_seconds, labels={"route": "GET /requests"}
)
async def get_requests(
    request: Request,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get requests for a user the given Authorization token"""
    app.state.web_http_requests_total.inc({"type": "GET /requests"})
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
