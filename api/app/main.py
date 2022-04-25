__version__ = "2.0"

from typing import Optional
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
import pika
from enum import Enum
from pydantic import BaseModel

from db.dbmanager.dbmanager import DBManager
from geoquery.geoquery import GeoQuery

from .components.access import AccessManager
from .components.dataset import DatasetManager
from .components.file import FileManager
from .util import UserCredentials


app = FastAPI()
# RabbitMQ Broker Connection
broker_conn = pika.BlockingConnection(pika.ConnectionParameters(host="broker"))
broker_chann = broker_conn.channel()


@app.get("/")
async def dds_info():
    return {f"DDS API {__version__}"}


@app.get("/datasets")
async def datasets(
    user_token: Optional[str] = Header(None, convert_underscores=True)
):
    user_cred = UserCredentials(user_token)
    return DatasetManager.get_eligible_products_for_all_datasets(
        user_credentials=user_cred
    )


@app.get("/datasets/{dataset_id}")
async def dataset(
    dataset_id: str,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
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
    return {f"estimate size for {dataset_id} {product_id} is 10GB"}


@app.post("/datasets/{dataset_id}/{product_id}/execute")
async def query(
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    format: Optional[str] = "netcdf",
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    # TODO: Validation Query Schema
    # TODO: estimate the size and will not execute if it is above the limit

    user_cred = UserCredentials(user_token)
    AccessManager.authenticate_user(user_cred)
    if user_cred.is_public:
        raise HTTPException(
            status_code="401",
            detail="Anonymouse user cannot execute queries! Please log in!",
        )
    request_id = DBManager().create_request(
        user_id=user_cred.id,
        dataset=dataset_id,
        product=product_id,
        query=query.json(),
    )

    # TODO: find a separator; for the moment use "\"
    message = (
        f"{request_id}\\{dataset_id}\\{product_id}\\{query.json()}\\{format}"
    )

    # submit request to broker queue
    broker_chann.basic_publish(
        exchange="",
        routing_key="query_queue",
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ),
    )
    return request_id


@app.get("/requests")
async def get_requests(
    user_token: Optional[str] = Header(None, convert_underscores=True)
):
    user_cred = UserCredentials(user_token)
    return


@app.get("/requests/{request_id}/status")
async def get_request_status(request_id: int,):
    status = DBManager().get_request_status(request_id)
    if status is None:
        raise HTTPException(
            status_code=400,
            detail=f"Request with id: {request_id} does not exist!",
        )
    return {status.value: status.name}


@app.get("/download/{request_id}")
async def download_request_result(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
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
            detail=f"User with id: {user_id} is not authorized for results of the request with id {request_id}",
        )


@app.get("/requests/{request_id}/uri")
async def get_request_uri(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    return


@app.delete("/requests/{request_id}/")
async def get_request_uri(
    request_id: int,
    user_token: Optional[str] = Header(None, convert_underscores=True),
):
    return
