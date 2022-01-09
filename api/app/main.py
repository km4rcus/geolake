from fastapi import FastAPI
import pika
from enum import Enum
from pydantic import BaseModel
from db.dbmanager.dbmanager import DBManager
from geoquery.geoquery import GeoQuery

app = FastAPI()
db_conn = None
##
# RabbitMQ Broker Connection
broker_conn = pika.BlockingConnection(pika.ConnectionParameters(host='broker'))
broker_chann = broker_conn.channel()

@app.get("/")
async def dds_info():
    return {"DDS API 2.0"}

@app.get("/datasets")
async def datasets():
    return {"List of Datasets"}

@app.get("/datasets/{dataset_id}")
async def dataset(dataset_id: str):
    return {f"Dataset Info {dataset_id}"}

@app.get("/datasets/{dataset_id}/{product_id}")
async def dataset(dataset_id: str, product_id: str):
    return {f"Product Info {product_id} from dataset {dataset_id}"}

@app.post("/datasets/{dataset_id}/{product_id}/estimate")
async def estimate(dataset_id: str, product_id: str, query: GeoQuery):
    return {f'estimate size for {dataset_id} {product_id} is 10GB'}

@app.post("/datasets/{dataset_id}/{product_id}/execute")
async def query(dataset_id: str, product_id: str, format: str, query: GeoQuery):
    global db_conn
    if not db_conn:
        db_conn = DBManager()
#
# 
# TODO: Validation Query Schema
# TODO: estimate the size and will not execute if it is above the limit
#
# 
    request_id = db_conn.create_request(dataset=dataset_id, product=product_id, query=query.json())
    print(f"request id: {request_id}")

# we should find a separator; for the moment use "\"
    message = f'{request_id}\\{dataset_id}\\{product_id}\\{query.json()}\\{format}'

# submit request to broker queue
    broker_chann.basic_publish(
        exchange='',
        routing_key='query_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    return request_id

@app.get("/requests")
async def get_requests():
    return 

@app.get("/requests/{request_id}/status")
async def get_request_status(request_id: int):
    return db_conn.get_request_status(request_id)

@app.get("/requests/{request_id}/uri")
async def get_request_uri(request_id: int):
    return