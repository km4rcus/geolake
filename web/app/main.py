"""Endpoints for `web` component"""
__version__ = "2.0"
import os
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import Response
from geoquery.geoquery import GeoQuery

from .access import AccessManager
from .converter import Converter
from .dataset import DatasetManager
from .requester import Requester
from .exceptions import (
    AuthenticationFailed,
    AuthorizationFailed,
    MissingKeyInCatalogEntryError,
)

_pref = os.environ.get("ENDPOINT_PREFIX", "/web")
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
    docs_url=f"{_pref}/docs",
    openapi_url=f"{_pref}/openapi.json",
)
app.router.prefix = _pref

Converter.load_templates()
Requester.init()


@app.get("/")
async def dds_info(req: Request):
    """Return current version of the DDS API for the Webportal"""
    return req.scope.get("root_path")
    return f"DDS Webportal API {__version__}"


@app.get("/datasets")
async def get_datasets(
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get list of eligible datasets for the home page of the Webportal"""
    try:
        user_credentials = AccessManager.retrieve_credentials_from_jwt(
            authorization
        )
        datasets = DatasetManager.get_datasets_and_eligible_products_names(
            user_credentials=user_credentials
        )
    except AuthenticationFailed as err:
        raise HTTPException(
            status_code=401, detail="User could not be authenticated"
        ) from err
    else:
        return Response(
            content=Converter.render_list_datasets(datasets),
            media_type="application/json",
        )


@app.get("/datasets/{dataset_id}/{product_id}")
async def get_details_product(
    dataset_id: str,
    product_id: str,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get details for Webportal"""
    try:
        user_credentials = AccessManager.retrieve_credentials_from_jwt(
            authorization
        )
        details = DatasetManager.get_details_for_product_if_eligible(
            dataset_id=dataset_id,
            product_id=product_id,
            user_credentials=user_credentials,
        )
    except AuthenticationFailed as err:
        raise HTTPException(
            status_code=401, detail="User could not be authenticated"
        ) from err
    except AuthorizationFailed as err:
        raise HTTPException(
            status_code=401, detail="User is not authorized"
        ) from err
    except MissingKeyInCatalogEntryError as err:
        raise HTTPException(
            status_code=500,
            detail=(
                f"dataset '{err.dataset}' catalog entry does not contain"
                " '{err.key}' key"
            ),
        ) from err
    else:
        return details
        return Response(
            content=Converter.render_details(details),
            media_type="application/json",
        )


@app.post("/datasets/{dataset_id}/{product_id}/execute")
async def execute(
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    format: Optional[str] = "netcdf",
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Schedule the job of data retrieving by using geokube-dds API"""
    try:
        user_credentials = AccessManager.retrieve_credentials_from_jwt(
            authorization
        )
        response = await Requester.post(
            url=f"/datasets/{dataset_id}/{product_id}/execute",
            data=query,
            params={"format": format},
            user_credentials=user_credentials,
        )
    except AuthenticationFailed as err:
        raise HTTPException(
            status_code=401, detail="User could not be authenticated"
        ) from err
    else:
        return response


@app.post("/datasets/{dataset_id}/{product_id}/estimate")
async def estimate(
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Estimate the resulting size of the query by using geokube-dds API"""
    try:
        user_credentials = AccessManager.retrieve_credentials_from_jwt(
            authorization
        )
        resposen = await Requester.post(
            url=f"/datasets/{dataset_id}/{product_id}/estimate",
            data=query,
            user_credentials=user_credentials,
        )
    except AuthenticationFailed as err:
        raise HTTPException(
            status_code=401, detail="User could not be authenticated"
        ) from err
    else:
        return resposen
