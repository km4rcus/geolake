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
import time
import pika
import logging
import asyncio
from dask.distributed import Client, LocalCluster, Nanny, Status

import threading, functools

from geokube.core.datacube import DataCube

from datastore.datastore import Datastore
from db.dbmanager.dbmanager import DBManager, RequestStatus

from meta import LoggableMeta

_BASE_DOWNLOAD_PATH = "/downloads"


def ds_query(ds_id, prod_id, query, compute, request_id):
    res_path = os.path.join(_BASE_DOWNLOAD_PATH, request_id)
    os.makedirs(res_path, exist_ok=True)
    ds = Datastore()
    kube = ds.query(ds_id, prod_id, query, compute)
    if isinstance(kube, DataCube):
        return kube.persist(res_path)
    else:
        return kube.persist(res_path, zip_if_many=True)


class Executor(metaclass=LoggableMeta):
    _LOG = logging.getLogger("geokube.Executor")

    def __init__(self, broker, store_path, cache_path):
        self._datastore = Datastore(cache_path=cache_path)
        self._store = store_path
        broker_conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=broker, heartbeat=10),
        )
        self._conn = broker_conn
        self._channel = broker_conn.channel()
        self._db = DBManager()

    def create_dask_cluster(self, dask_cluster_opts: dict = None):
        if dask_cluster_opts is None:
            dask_cluster_opts = {}
            dask_cluster_opts["scheduler_port"] = int(
                os.getenv("DASK_SCHEDULER_PORT", 8188)
            )
            port = int(os.getenv("DASK_DASHBOARD_PORT", 8787))
            dask_cluster_opts["dashboard_address"] = f":{port}"
            dask_cluster_opts["n_workers"] = int(
                os.getenv("DASK_N_WORKERS", 1)
            )
        self._worker_id = self._db.create_worker(
            status="enabled",
            dask_scheduler_port=dask_cluster_opts["scheduler_port"],
            dask_dashboard_address=dask_cluster_opts["dashboard_address"],
        )
        self._LOG.info(
            "creating Dask Cluster with options: `%s`",
            dask_cluster_opts,
            extra={"track_id": self._worker_id},
        )
        dask_cluster = LocalCluster(
            n_workers=dask_cluster_opts["n_workers"],
            scheduler_port=dask_cluster_opts["scheduler_port"],
            dashboard_address=dask_cluster_opts["dashboard_address"],
        )
        self._LOG.info(
            "creating Dask Client...", extra={"track_id": self._worker_id}
        )
        self._dask_client = Client(dask_cluster)
        self._nanny = Nanny(self._dask_client.cluster.scheduler.address)

    def maybe_restart_cluster(self):
        if self._dask_client.cluster.status is Status.failed:
            self._LOG.info("attempt to restart the cluster...")
            try:
                self._dask_client.restart(wait_for_workers=False)
                asyncio.run(self._nanny.restart())
            except Exception as err:
                self._LOG.error(
                    "couldn't restart the cluster due to an error: %s", err
                )
                self._LOG.info("closing the cluster")
                self._dask_client.cluster.close()
        if self._dask_client.cluster.status is Status.closed:
            self._LOG.info("recreating the cluster")
            self.create_dask_cluster()

    def ack_message(self, channel, delivery_tag):
        """Note that `channel` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            self._LOG.info(
                "cannot acknowledge the message. channel is closed!"
            )
            pass

    def query(self, connection, channel, delivery_tag, body):
        m = body.decode().split("\\")
        request_id = m[0]
        dataset_id = m[1]
        product_id = m[2]
        query = m[3]
        self._LOG.debug(
            "executing query: `%s`", body, extra={"track_id": request_id}
        )

        # TODO: estimation size should be updated, too
        self._db.update_request(
            request_id=request_id,
            worker_id=self._worker_id,
            status=RequestStatus.RUNNING,
        )
        self._LOG.debug(
            "submitting job for request", extra={"track_id": request_id}
        )
        future = self._dask_client.submit(
            ds_query,
            ds_id=dataset_id,
            prod_id=product_id,
            query=query,
            compute=False,
            request_id=request_id,
        )
        status = fail_reason = location_path = None
        try:
            self._LOG.debug(
                "attempt to get result for the request",
                extra={"track_id": request_id},
            )
            for _ in range(int(os.environ.get("RESULT_CHECK_RETRIES", 30))):
                if future.done():
                    self._LOG.debug(
                        "result is done",
                        extra={"track_id": request_id},
                    )
                    location_path = future.result()
                    break
                self._LOG.debug(
                    "result is not ready yet. sleeping 30 sec",
                    extra={"track_id": request_id},
                )
                time.sleep(int(os.environ.get("SLEEP_SEC", 30)))
            else:
                self._LOG.info(
                    "processing timout",
                    extra={"track_id": request_id},
                )
                future.cancel()
                status = RequestStatus.FAILED
                fail_reason = "Processing timeout"
        except Exception as e:
            self._LOG.error(
                "failed to get result due to an error: %s",
                e,
                exc_info=True,
                stack_info=True,
                extra={"track_id": request_id},
            )
            status = RequestStatus.FAILED
            fail_reason = f"{type(e)}: {str(e)}"
        else:
            if location_path:
                self._LOG.debug(
                    "updating status and download URI for request",
                    extra={"track_id": request_id},
                )
                status = RequestStatus.DONE
            elif status is not RequestStatus.FAILED:
                self._LOG.warning(
                    "location path is `None` - resulting dataset was empty!",
                    extra={"track_id": request_id},
                )
                status = RequestStatus.FAILED
                fail_reason = (
                    "the query resulted in an empty Dataset. Check your"
                    " request!"
                )
        self._db.update_request(
            request_id=request_id,
            worker_id=self._worker_id,
            status=status,
            location_path=location_path,
            size_bytes=self.get_size(location_path),
            fail_reason=fail_reason,
        )
        self._LOG.debug(
            "acknowledging request", extra={"track_id": request_id}
        )
        cb = functools.partial(self.ack_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

        if status is RequestStatus.FAILED:
            self.maybe_restart_cluster()
        self._LOG.debug("request acknowledged", extra={"track_id": request_id})

    def on_message(self, channel, method_frame, header_frame, body, args):
        (connection, threads) = args
        delivery_tag = method_frame.delivery_tag
        t = threading.Thread(
            target=self.query, args=(connection, channel, delivery_tag, body)
        )
        t.start()
        threads.append(t)

    def subscribe(self, etype):
        self._LOG.debug(
            "subscribe channel: %s_queue", etype, extra={"track_id": "N/A"}
        )
        self._channel.queue_declare(queue=f"{etype}_queue", durable=True)
        self._channel.basic_qos(prefetch_count=1)

        threads = []
        on_message_callback = functools.partial(
            self.on_message, args=(self._conn, threads)
        )

        self._channel.basic_consume(
            queue=f"{etype}_queue", on_message_callback=on_message_callback
        )

    def listen(self):
        while True:
            self._channel.start_consuming()

    def get_size(self, location_path):
        if location_path and os.path.exists(location_path):
            return os.path.getsize(location_path)
        return None


if __name__ == "__main__":
    broker = os.getenv("BROKER", "broker")
    executor_types = os.getenv("EXECUTOR_TYPES", "query").split(",")
    store_path = os.getenv("STORE_PATH", ".")
    cache_path = os.getenv("CACHE_PATH", ".")

    executor = Executor(
        broker=broker, store_path=store_path, cache_path=cache_path
    )
    print("channel subscribe")
    for etype in executor_types:
        if etype == "query":
            executor.create_dask_cluster()

        executor.subscribe(etype)

    print("waiting for requests ...")
    executor.listen()
