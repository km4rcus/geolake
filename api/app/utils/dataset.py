import logging
from typing import Optional

from ..exceptions import (
    MissingDatasetError,
    MissingKeyInCatalogEntryError,
)
from ..datastore.datastore import Datastore

log = logging.getLogger("geokube.utils.dataset")


def load_cache():
    """Load Datastore cache"""
    Datastore()._load_cache()


def assert_product_exists(dataset_id, product_id: Optional[str] = None):
    """Assert that the dataset or product exist.
    If `product_id` is set, the method verifies if it belongs to the
    dataset with ID in `dataset_id`.

    Parameters
    ----------
    dataset_id : str
        ID of the dataset
    product_id : str, optional
        ID of the product

    Raises
    -------
    MissingDatasetError
        If dataset is not defined
    MissingKeyInCatalogEntryError
        If product is not defined for the dataset
    """
    if dataset_id not in Datastore().dataset_list():
        log.info(
            "requested dataset: `%s` was not found in the catalog!",
            dataset_id,
        )
        raise MissingDatasetError(dataset_id)
    if product_id is not None:
        if product_id not in Datastore().product_list(dataset_id):
            log.info(
                "requested product: `%s` for dataset: `%s` was not found"
                " in the catalog!",
                product_id,
                dataset_id,
            )
            raise MissingKeyInCatalogEntryError(product_id, dataset_id)
