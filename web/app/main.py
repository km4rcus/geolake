__version__ = "2.0"

from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from enum import Enum
from pydantic import BaseModel

from .access import AccessManager
from .converter import Converter
from .util import UserCredentials


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
    """Get list of eligible datasets for Webportal

    Returns
    -------
    datasets : str
        Datasets with eligible products listed

    """
    # user_credentials = AccessManager.retrieve_credentials_from_jwt(authorization)
    user_credentials = AccessManager.retrieve_credentials_from_jwt(
        authorization
    )
    datasets = AccessManager.get_datasets_and_eligible_products_names(
        user_credentials=user_credentials
    )
    return Converter.render_list_datasets(datasets)


@app.get("/datasets/{dataset_id}")
async def get_details_for_dataset(
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
    details = AccessManager.get_details_for_eligible_products_for_dataset(
        dataset_id=dataset_id, user_credentials=user_credentials
    )
    return Converter.render_details(details)


@app.get("/datasets/{dataset_id}/{product_id}")
async def get_details_for_product(
    dataset_id: str,
    product_id: str,
    authorization: Optional[str] = Header(None, convert_underscores=True),
):
    """Get details for Webportal

    Parameters
    ----------
    dataset_id : str
        Name of the dataset
    product : str
        Name of the product

    Returns
    -------
    details : str
        Details for the dataset indicated by `dataset_id` parameter

    """
    user_credentials = AccessManager.retrieve_credentials_from_jwt(
        authorization
    )
    details = AccessManager.get_details_for_product_if_eligible(
        dataset_id=dataset_id,
        product_id=product_id,
        user_credentials=user_credentials,
    )
    return Converter.render_details(details)
