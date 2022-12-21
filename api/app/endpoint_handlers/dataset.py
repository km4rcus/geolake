"""Modules realizing logic for dataset-related endpoints"""
import pika
from typing import Optional

from db.dbmanager.dbmanager import DBManager
from geoquery.geoquery import GeoQuery

from ..auth import Context
from ..auth.manager import (
    is_role_eligible_for_product,
    assert_is_role_eligible,
)
from ..auth import assert_not_anonymous
from ..api_logging import get_dds_logger
from .. import exceptions as exc
from ..utils import make_bytes_readable_dict
from ..metrics import log_execution_time
from ..validation import assert_product_exists
from ..datastore.datastore import Datastore


log = get_dds_logger(__name__)
data_store = Datastore(cache_path="/cache")


@log_execution_time(log)
def get_datasets(context: Context) -> list[dict]:
    """Realize the logic for the endpoint:

    `GET /datasets`

    Get datasets names, their metadata and products names (if eligible for a user).
    If no eligible products are found for a dataset, it is not included.

    Parameters
    ----------
    context : Context
        Context of the current http request

    Returns
    -------
    datasets : list of dict
        A list of dictionaries with datasets information (including metadata and
        eligible products lists)

    Raises
    -------
    MissingKeyInCatalogEntryError
        If the dataset catalog entry does not contain the required key
    """
    log.debug(
        "getting all eligible products for datasets...",
        extra={"rid": context.rid},
    )
    user_roles_names = DBManager().get_user_roles_names(context.user.id)
    datasets = []
    for dataset_id in data_store.dataset_list():
        log.debug(
            "getting info and eligible products for `%s`",
            dataset_id,
            extra={"rid": context.rid},
        )
        dataset_info = data_store.dataset_info(dataset_id=dataset_id)
        try:
            eligible_prods = {
                prod_name: prod_info
                for prod_name, prod_info in dataset_info["products"].items()
                if is_role_eligible_for_product(
                    context=context,
                    product_role_name=prod_info.get("role"),
                    user_roles_names=user_roles_names,
                )
            }
        except KeyError as err:
            log.error(
                "dataset `%s` does not have products defined",
                dataset_id,
                exc_info=True,
                extra={"rid": context.rid},
            )
            raise exc.MissingKeyInCatalogEntryError(
                key="products", dataset=dataset_id
            ) from err
        else:
            if len(eligible_prods) == 0:
                log.debug(
                    "no eligible products for dataset `%s` for the user"
                    " `%s`. dataset skipped",
                    dataset_id,
                    context.user.id,
                    extra={"rid": context.rid},
                )
            else:
                dataset_info["products"] = eligible_prods
                datasets.append(dataset_info)
    return datasets


@log_execution_time(log)
@assert_product_exists
def get_product_details(
    context: Context, dataset_id: str, product_id: str
) -> dict:
    """Realize the logic for the endpoint:

    `GET /datasets/{dataset_id}/{product_id}`

    Get details for the given product indicated by `dataset_id`
    and `product_id` arguments.

    Parameters
    ----------
    context : Context
        Context of the current http request
    dataset_id : str
        ID of the dataset
    product_id : str
        ID of the dataset

    Returns
    -------
    details : dict
        Details for the given product

    Raises
    -------
    AuthorizationFailed
        If user is not authorized for the resources
    """
    log.debug(
        "getting details for eligible products of `%s`",
        dataset_id,
        extra={"rid": context.rid},
    )
    user_roles_names = DBManager().get_user_roles_names(context.user.id)
    details = data_store.product_details(
        dataset_id=dataset_id, product_id=product_id, use_cache=True
    )
    assert_is_role_eligible(
        context=context,
        product_role_name=details["metadata"].get("role"),
        user_roles_names=user_roles_names,
    )
    return details


@log_execution_time(log)
@assert_product_exists
def get_metadata(context: Context, dataset_id: str, product_id: str):
    """Realize the logic for the endpoint:

    `GET /datasets/{dataset_id}/{product_id}/metadata`

    Get metadata for the product.

    Parameters
    ----------
    context : Context
        Context of the current http request
    dataset_id : str
        ID of the dataset
    product_id : str
        ID of the product
    """
    log.debug(
        "getting metadata for '{dataset_id}.{product_id}'",
        extra={"rid": context.rid},
    )
    return data_store.product_metadata(dataset_id, product_id)


@log_execution_time(log)
@assert_product_exists
def estimate(
    context: Context,
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    unit: Optional[str] = None,
):
    """Realize the logic for the nedpoint:

    `POST /datasets/{dataset_id}/{product_id}/estimate`

    Estimate the size of the resulting data.
    No authentication is needed for estimation query.

    Parameters
    ----------
    dataset_id : str
        ID of the dataset
    product_id : str
        ID of the product
    query : GeoQuery
        Query to perform
    unit : str
        One of unit [bytes, kB, MB, GB] to present the result. If `None`,
        unit will be inferred.

    Returns
    -------
    size_details : dict
        Estimated size of  the query in the form:
        ```python
        {
            "value": val,
            "units": units
        }
        ```
    """
    query_bytes_estimation = data_store.query(
        dataset_id, product_id, query, compute=False
    ).nbytes
    return make_bytes_readable_dict(
        size_bytes=query_bytes_estimation, units=unit
    )


@log_execution_time(log)
@assert_not_anonymous
@assert_product_exists
def query(
    context: Context,
    dataset_id: str,
    product_id: str,
    query: GeoQuery,
    format: str,
):
    """Realize the logic for the endpoint:

    `POST /datasets/{dataset_id}/{product_id}/execute`

    Query the data and return the ID of the request.

    Parameters
    ----------
    context : Context
        Context of the current http request
    dataset_id : str
        ID of the dataset
    product_id : str
        ID of the product
    query : GeoQuery
        Query to perform
    format : str
        Format of the data

    Returns
    -------
    request_id : int
        ID of the request

    Raises
    -------
    AuthorizationFailed
    """
    log.debug("geoquery: %s", query, extra={"rid": context.rid})
    estimated_size = estimate(
        context, dataset_id, product_id, query, "GB"
    ).get("value")
    allowed_size = data_store.product_metadata(dataset_id, product_id).get(
        "maximum_query_size_gb", 10
    )
    if estimated_size > allowed_size:
        raise exc.MaximumAllowedSizeExceededError(
            dataset_id=dataset_id,
            product_id=product_id,
            estimated_size_gb=estimated_size,
            allowed_size_gb=allowed_size,
        )
    broker_conn = pika.BlockingConnection(
        pika.ConnectionParameters(host="broker")
    )
    broker_channel = broker_conn.channel()

    request_id = DBManager().create_request(
        user_id=context.user.id,
        dataset=dataset_id,
        product=product_id,
        query=query.original_query_json(),
    )

    # TODO: find a separator; for the moment use "\"
    message = (
        f"{request_id}\\{dataset_id}\\{product_id}\\{query.json()}\\{format}"
    )

    broker_channel.basic_publish(
        exchange="",
        routing_key="query_queue",
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ),
    )
    broker_conn.close()
    return request_id
