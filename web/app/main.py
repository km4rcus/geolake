"""Endpoints for `web` component"""
__version__ = "2.0"

from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import PlainTextResponse
from geoquery.geoquery import GeoQuery

from .access import AccessManager
from .converter import Converter
from .dataset import DatasetManager


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
)

Converter.load_templates()


@app.get("/")
async def dds_info():
    """Return current version of the DDS API for the Webportal

    Returns
    -------
    version : str
        A string including version of DDS

    """
    return f"DDS Webportal API {__version__}"


@app.get("/datasets")
async def get_datasets(
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get list of eligible datasets for the home page of the Webportal

    Returns
    -------
    datasets : str
        Datasets with eligible products listed

    """
    user_credentials = AccessManager.retrieve_credentials_from_jwt(
        authorization
    )
    datasets = DatasetManager.get_datasets_and_eligible_products_names(
        user_credentials=user_credentials
    )
    # NOTE: we use PlainTextResponse as Jinja render already returns JSON string
    return PlainTextResponse(Converter.render_list_datasets(datasets))


@app.get("/datasets/{dataset_id}")
async def get_details_for_product(
    dataset_id: str,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get details for Webportal

    Parameters
    ----------
    dataset_id : str
        Name of the dataset

    Returns
    -------
    details : str
        Details for the dataset indicated by `dataset_id` parameter

    """
    user_credentials = AccessManager.retrieve_credentials_from_jwt(
        authorization
    )
    details = DatasetManager.get_details_for_dataset_products_if_eligible(
        dataset_id=dataset_id,
        user_credentials=user_credentials,
    )
    return Converter.render_details(details)


@app.post("/datasets/{dataset_id}/{product_id}/execute")
async def execute(
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    format: Optional[str] = "netcdf",
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Schedule the job of data retrieving.

    Parameters
    ----------
    dataset_id : str
        ID of the dataset in catalog
    product_id : str
        ID of the product for the requested dataset (must be included for dataset with id dataset_id)
    query : GeoQuery
        Query for which data should be extracted
    format : str, optional
        Format of the resulting file, default: netcdf
    authorization : str, optional
        Header containing authorization token

    Returns
    -------
    request_id : int
        ID of the scheduled request
    """
    user_credentials = AccessManager.retrieve_credentials_from_jwt(
        authorization
    )
    return DatasetManager.retrieve_data_and_get_request_id(
        dataset_id=dataset_id,
        product_id=product_id,
        user_credentials=user_credentials,
        query=query,
        format=format,
    )
