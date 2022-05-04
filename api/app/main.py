__version__ = "2.0"

from typing import Optional
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from enum import Enum
from pydantic import BaseModel

from db.dbmanager.dbmanager import DBManager
from geoquery.geoquery import GeoQuery

from .components.access import AccessManager
from .components.dataset import DatasetManager
from .components.file import FileManager
from .components.request import RequestManager
from .util import UserCredentials


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
)


@app.get("/")
async def dds_info():
    """Return current version of the DDS API

    Returns
    -------
    version : str
        A string including version of DDS

    """
    return f"DDS API {__version__}"


@app.get("/datasets")
async def datasets(
    user_token: Optional[str] = Header(None, convert_underscores=True)
):
    """List all products eligible for a user defined by user_token

    Parameters
    ----------
    user_token : str, optional
        User token in the form <user_id>:<user_key>

    Returns
    -------
    datasets : dict
        A dictionary where keys are names of datasets and values are list of eligible products for the given dataset

    Raises
    ------
    HTTPException
        400 if user was not authenticated properly
    """
    user_cred = UserCredentials(user_token)
    return DatasetManager.get_eligible_products_for_all_datasets(
        user_credentials=user_cred
    )


@app.get("/datasets/{dataset_id}")
async def dataset(
    dataset_id: str,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Get eligible products for the given dataset

    Parameters
    ----------
    dataset_id : str
        ID of the dataset for which eligible products should be returned
    user_token : str, optional
        User token in the form <user_id>:<user_key>

    Returns
    -------
    products : list
        List of products eligible for the user

    Raises
    ------
    HTTPException
        400 if user was not authenticated properly
    """
    user_cred = UserCredentials(user_token)
    return DatasetManager.get_eligible_products_for_dataset(
        user_credentials=user_cred, dataset_id=dataset_id
    )


@app.get("/datasets/{dataset_id}/{product_id}")
async def dataset(
    dataset_id: str,
    product_id: str,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Get details for the requested product if user is authorized.

    Parameters
    ----------
    dataset_id : str
        ID of the dataset in catalog
    product_id : str
        ID of the product for the requested dataset (must be included for dataset with id dataset_id)
    user_token : str, optional
        User token in the form <user_id>:<user_key>

    Returns
    -------
    details : dict
        Dictionary of details for the requested product

    Raises
    ------
    HTTPException
        400 if user was not authenticated properly
        401 if user is not authorized for the product
    """
    user_cred = UserCredentials(user_token)
    return DatasetManager.get_details_if_product_eligible(
        user_credentials=user_cred,
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
    """Estimate the resulting size of the query

    Parameters
    ----------
    dataset_id : str
        ID of the dataset in catalog
    product_id : str
        ID of the product for the requested dataset (must be included for dataset with id dataset_id)
    query : GeoQuery
        Query for which estimation should be done
    user_token : str, optional
        User token in the form <user_id>:<user_key>

    Returns
    -------
    estimate : dict
        Dictionary representing estimated size of a result. It contains value and associated unit: bytes, kB, MB, GB
    """
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
    """Schedule the job of data retrieving.

    Parameters
    ----------
    dataset_id : str
        ID of the dataset in catalog
    product_id : str
        ID of the product for the requested dataset (must be included for dataset with id dataset_id)
    query : GeoQuery
        Query for which estimation should be done
    format : str, optional
        Format of the resulting file, default: netcdf
    user_token : str, optional
        User token in the form <user_id>:<user_key>

    Returns
    -------
    request_id : int
        ID of the scheduled request

    Raises
    ------
    HTTPException
        400 if user was not authenticated properly
        401 if user is anonymous
    """
    # TODO: Validation Query Schema
    # TODO: estimate the size and will not execute if it is above the limit
    user_cred = UserCredentials(user_token)
    return DatasetManager.retrieve_data_and_get_request_id(
        user_credentials=user_cred,
        dataset_id=dataset_id,
        product_id=product_id,
        query=query,
        format=format,
    )


@app.get("/requests")
async def get_requests(
    user_token: Optional[str] = Header(None, convert_underscores=True)
):
    """Get all requests for the user.

    Parameters
    ----------
    user_token : str, optional
        User token in the form <user_id>:<user_key>

    Returns
    -------
    requests : list
        List of requests executed by the user

    Raises
    ------
    HTTPException
        400 if user was not authenticated properly
        401 if user is anonymous
    """
    user_cred = UserCredentials(user_token)
    AccessManager.authenticate_user(user_cred)
    return RequestManager.get_requests_details_for_user(
        user_credentials=user_cred
    )


@app.get("/requests/{request_id}/status")
async def get_request_status(request_id: int):
    """Get status of the request without authentication.

    Parameters
    ----------
    request_id : int
        ID of a request for which status should be returned

    Returns
    -------
    status : dict
        Dictionary representing enum value and member name of the status, e.g. {3: "DONE"}

    Raises
    ------
    HTTPException
        400 if user was not authenticated properly or request with request_id does not exist
    """
    # NOTE: no auth required for checking status
    status = RequestManager.get_request_status_for_request_id(
        request_id=request_id
    )
    return {status.value: status.name}


@app.get("/download/{request_id}")
async def download_request_result(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Download result of the resuest.

    Parameters
    ----------
    request_id : int
        ID of the request for which file was generated
    user_token : str, optional
        User token in the form <user_id>:<user_key>

    Returns
    -------
    file_response : FileResponse
        Stream of file to be downloaded

    Raises
    ------
    HTTPException
        400 if user was not authenticated properly
        401 if user is not authorized for the requested resource
    """
    user_cred = UserCredentials(user_token)
    AccessManager.authenticate_user(user_cred)
    if AccessManager.is_user_eligible_for_request(
        user_credentials=user_cred, request_id=request_id
    ):
        path = FileManager.prepare_request_for_download_and_get_path(
            request_id=request_id
        )
        return FileResponse(path=path, filename=path)
    else:
        raise HTTPException(
            status_code=401,
            detail=(
                f"User with id: {user_cred.id} is not authorized for results"
                f" of the request with id {request_id}"
            ),
        )


@app.get("/requests/{request_id}/uri")
async def get_request_uri(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    """Get download URI for the requst

    Parameters
    ----------
    request_id : int
        ID of the request for which file was generated
    user_token : str, optional
        User token in the form <user_id>:<user_key>

    Returns
    -------
    file_uri : str
        URI of the file being created by the request

    Raises
    ------
    HTTPException
        400 if user was not authenticated properly
    """
    user_cred = UserCredentials(user_token)
    AccessManager.authenticate_user(user_cred)
    return RequestManager.get_request_uri_for_request_id(request_id=request_id)


@app.delete("/requests/{request_id}/")
async def get_request_uri(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    # TODO:
    raise HTTPException(status_code=400, detail="NotImplementedError")
