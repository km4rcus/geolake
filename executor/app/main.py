# We have three type of executor:
# - query executor (query)
# - estimate query executor (estimate)
# - catalog info executor (info)
#
# Configuration parameters for the executor:
#    type: query, estimate, catalog
#    dask cluster base ports (if they are not provided the cluster is not created: (e.g. for estimate and catalog info))
#    channel: channel_queue, channel_type, channel_durable
#    catalog path
#    store_path (where to store the query results)
#
# An executor will register to the DB and get a worker id
# if dask cluster base ports are provided, a dask cluster is created
# an executor mush have a unique port for the dask scheduler/dashboard

import os
import json
import pika
import logging
from dask.distributed import Client, LocalCluster

from datastore.datastore import Datastore
from db.dbmanager.dbmanager import DBManager, RequestStatus

_BASE_DOWNLOAD_PATH = os.path.join("..", "downloads")


def ds_query(ds_id, prod_id, query, compute, request_id):
    res_path = os.path.join(_BASE_DOWNLOAD_PATH, request_id)
    os.makedirs(res_path, exist_ok=True)
    ds = Datastore()
    kube = ds.query(ds_id, prod_id, query, compute)

    kube.persist(res_path)
    # after closing https://github.com/geokube/geokube/issues/146
    # kube.persist(res_path, zip_if_many=True)
    return kube


class Executor:

    _LOG = logging.getLogger("Executor")

    def __init__(self, broker, store_path):
        self._datastore = Datastore()
        self._store = store_path
        broker_conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=broker)
        )
        self._channel = broker_conn.channel()
        self._db = DBManager()

    def create_dask_cluster(self, dask_cluster_opts):
        self._worker_id = self._db.create_worker(
            status="enabled",
            dask_scheduler_port=dask_cluster_opts["scheduler_port"],
            dask_dashboard_address=dask_cluster_opts["dashboard_address"],
        )
        dask_cluster = LocalCluster(
            n_workers=dask_cluster_opts["n_workers"],
            scheduler_port=dask_cluster_opts["scheduler_port"],
            dashboard_address=dask_cluster_opts["dashboard_address"],
        )
        self._dask_client = Client(dask_cluster)

    def query_and_persist(self, ds_id, prod_id, query, compute, format):
        kube = self._datastore.query(ds_id, prod_id, query, compute)
        kube.persist(self._store, format=format)

    def estimate(self, channel, method, properties, body):
        m = body.decode().split("\\")
        dataset_id = m[0]
        product_id = m[1]
        query = m[2]
        kube = self._datastore.query(dataset_id, product_id, query)
        channel.basic_publish(
            exchange="",
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(
                correlation_id=properties.correlation_id
            ),
            body=str(kube.get_nbytes()),
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def info(self, channel, method, properties, body):
        m = body.decode().split("\\")
        oper = m[0]  # could be list or info
        if oper == "list":
            if len(m) == 1:  # list datasets
                response = json.loads(self._datastore.dataset_list())
            if len(m) == 2:  # list dataset products
                dataset_id = m[1]
                response = json.loads(self._datastore.product_list(dataset_id))

        if oper == "info":
            if len(m) == 2:  # dataset info
                dataset_id = m[1]
                response = json.loads(self._datastore.dataset_info(dataset_id))
            if len(m) == 3:  # product info
                dataset_id = m[1]
                product_id = m[2]
                response = json.loads(
                    self._datastore.product_info(dataset_id, product_id)
                )

        channel.basic_publish(
            exchange="",
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(
                correlation_id=properties.correlation_id
            ),
            body=response,
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def query(self, channel, method, properties, body):
        m = body.decode().split("\\")
        request_id = m[0]
        dataset_id = m[1]
        product_id = m[2]
        query = m[3]
        format = m[4]

        self._db.update_request(
            request_id=request_id,
            worker_id=self._worker_id,
            status=RequestStatus.RUNNING,
        )
        # future = self._dask_client.submit(self.query_and_persist, dataset_id, product_id, query, False, format)
        future = self._dask_client.submit(
            ds_query,
            ds_id=dataset_id,
            prod_id=product_id,
            query=query,
            compute=False,
            request_id=request_id,
        )
        try:
            future.result()
            self._db.update_request(
                request_id=request_id,
                worker_id=self._worker_id,
                status=RequestStatus.DONE,
                location_path=os.path.join(_BASE_DOWNLOAD_PATH, request_id),
            )
        except Exception as e:
            self._LOG.error(f"Failed due to error: {e}")
            self._db.update_request(
                request_id=request_id,
                worker_id=self._worker_id,
                status=RequestStatus.FAILED,
            )

        channel.basic_ack(delivery_tag=method.delivery_tag)

    def subscribe(self, etype):
        self._LOG.debug(f"Subscribe channel: {etype}_queue")
        self._channel.queue_declare(queue=f"{etype}_queue", durable=True)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue=f"{etype}_queue", on_message_callback=getattr(self, etype)
        )

    def listen(self):
        while True:
            self._channel.start_consuming()


if __name__ == "__main__":

    broker = os.getenv("BROKER", "broker")
    executor_types = os.getenv("EXECUTOR_TYPES", "query").split(",")
    store_path = os.getenv("STORE_PATH", ".")

    executor = Executor(broker=broker, store_path=store_path)
    print("channel subscribe")
    for etype in executor_types:
        if etype == "query":
            dask_cluster_opts = {}
            dask_cluster_opts["scheduler_port"] = int(
                os.getenv("DASK_SCHEDULER_PORT", 8188)
            )
            port = int(os.getenv("DASK_DASHBOARD_PORT", 8787))
            dask_cluster_opts["dashboard_address"] = f":{port}"
            dask_cluster_opts["n_workers"] = int(
                os.getenv("DASK_N_WORKERS", 1)
            )
            executor.create_dask_cluster(dask_cluster_opts)

        executor.subscribe(etype)

    print("waiting for requests ...")
    executor.listen()
