"""Module contains the class DatasetManager being responsible for
accessing data via datastore.Datastore and scheduling the request job"""
from __future__ import annotations

import logging

from db.dbmanager.dbmanager import DBManager

from .datastore.datastore import Datastore
from .utils import UserCredentials, log_execution_time
from .access import AccessManager
from .meta import LoggableMeta
from .exceptions import (
    MissingKeyInCatalogEntryError,
    NoEligibleProductInDatasetError,
)


class DatasetManager(metaclass=LoggableMeta):
    """The component for dataset management, including getting details
    and submitting a request"""

    _LOG = logging.getLogger("DatasetManager")
    _DATASTORE = Datastore()

    @classmethod
    @log_execution_time(_LOG)
    def wrap_estimate_size_message(
        cls, dataset_id: str, product_id: str, estimated_size_gb: int
    ):
        """Wrap estimate size result into the message expected by the Webportal

        Parameters
        ----------
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the product
        estimated_size_gb : int
            Estimated size in gigabytes

        Returns
        -------
        message : dict
            A dicitonary with keys `status` and `message`
        """
        status = "OK"
        # NOTE: set 10 GB as default limit
        maximum_allowed_size_gb = cls._DATASTORE.product_metadata(
            dataset_id, product_id
        ).get("maximum_query_size_gb", 10)
        if dataset_id in []:
            msg = "Size estimation is not currently supported for this dataset"
        if estimated_size_gb is None:
            status = "Error"
            msg = "Could not estimate the size for that dataset"
        if estimated_size_gb > maximum_allowed_size_gb:
            status = "Error"
            msg = (
                f"Estimated request size ({estimated_size_gb} GB) is more than"
                f" maximum allowed size ({maximum_allowed_size_gb} GB). Please"
                " review your query"
            )
        else:
            msg = f"Estimated request size: {estimated_size_gb} GB"
        return {"status": status, "message": msg}

    @classmethod
    @log_execution_time(_LOG)
    def get_datasets_and_eligible_products_names(
        cls, user_credentials: UserCredentials
    ) -> list:
        """Get datasets names, their metadata and products names (if eligible for a user).
        If no eligible products are found for a dataset, it is not included.

        Parameters
        ----------
        user_credentials : UserCredentials
            Current user credentials

        Returns
        -------
        datasets : list
            A list of datasets information (including metadata and
            eligible products lists)

        Raises
        -------
        MissingKeyInCatalogEntryError
            If the dataset catalog entry does not contain the required key
        """
        cls._LOG.debug("getting all eligible products for datasets...")
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        datasets = []
        for dataset_id in cls._DATASTORE.dataset_list():
            if dataset_id == "visir":
                cls._LOG.info(
                    "skipping `visir` dataset due to the error geokube/#253"
                )
                continue
            cls._LOG.debug(
                "getting info and eligible products for `%s`", dataset_id
            )
            dataset_info = cls._DATASTORE.dataset_info(dataset_id=dataset_id)
            try:
                datasets.append(
                    cls._get_dataset_information_from_details_dict(
                        dataset_dict=dataset_info,
                        user_role_name=user_role_name,
                        dataset_id=dataset_id,
                        user_credentials=user_credentials,
                    )
                )
            except NoEligibleProductInDatasetError:
                cls._LOG.debug(
                    f"dataset '{dataset_id}' will not be considered. no"
                    " eligible products for the user role name"
                    f" '{user_role_name}'"
                )
                continue
        return datasets

    @classmethod
    @log_execution_time(_LOG)
    def get_details_for_product_if_eligible(
        cls,
        dataset_id: str,
        product_id: str,
        user_credentials: UserCredentials,
    ) -> dict:
        """Get details for the given product indicated by `dataset_id`
        and `product_id` arguments.

        Parameters
        ----------
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the dataset
        user_credentials : UserCredentials
            Current user credentials

        Returns
        -------
        details : dict
            Details for the given product

        Raises
        -------
        MissingKeyInCatalogEntryError
            If the dataset catalog entry does not contain the required key
        """
        cls._LOG.debug(
            "getting details for eligible products of `%s`", dataset_id
        )
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        details = Datastore().product_details(
            dataset_id=dataset_id, product_id=product_id, use_cache=True
        )
        AccessManager.assert_is_role_eligible(
            product_role_name=details["metadata"].get("role"),
            user_role_name=user_role_name,
        )
        return details

    @classmethod
    @log_execution_time(_LOG)
    def _get_dataset_information_from_details_dict(
        cls,
        dataset_dict: dict,
        user_role_name: str,
        dataset_id: str,
        user_credentials: UserCredentials,
    ) -> dict:
        cls._LOG.debug(
            "getting all eligible products for dataset: `%s`", dataset_id
        )
        try:
            eligible_prods = {
                prod_name: prod_info
                for prod_name, prod_info in dataset_dict["products"].items()
                if AccessManager.is_role_eligible_for_product(
                    product_role_name=prod_info.get("role"),
                    user_role_name=user_role_name,
                )
            }
        except KeyError as err:
            cls._LOG.error(
                "dataset `%s` does not have products defined",
                dataset_id,
                exc_info=True,
            )
            raise MissingKeyInCatalogEntryError(
                key="products", dataset=dataset_id
            ) from err
        else:
            if len(eligible_prods) == 0:
                cls._LOG.debug(
                    "no eligible products for dataset `%s` for the user"
                    " `%s`. dataset skipped",
                    dataset_id,
                    user_credentials.id,
                )
                raise NoEligibleProductInDatasetError(
                    dataset_id=dataset_id, user_role_name=user_role_name
                )
            else:
                dataset_dict["products"] = eligible_prods
        return dataset_dict
