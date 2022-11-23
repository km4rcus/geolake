"""Module for catalog management classes and functions"""
from __future__ import annotations

import os
import logging
import json

import intake

from geoquery.geoquery import GeoQuery

from geokube.core.datacube import DataCube
from geokube.core.dataset import Dataset

from .singleton import Singleton
from .util import log_execution_time


class Datastore(metaclass=Singleton):
    """Singleton component for managing catalog data"""

    _LOG = logging.getLogger("Datastore")

    def __init__(self, cache_path: str = "./") -> None:
        if "CATALOG_PATH" not in os.environ:
            self._LOG.error(
                "missing required environment variable: 'CATALOG_PATH'"
            )
            raise KeyError(
                "Missing required environment variable: 'CATALOG_PATH'"
            )
        cat = intake.open_catalog(os.environ["CATALOG_PATH"])
        #        self.catalog = cat(CACHE_DIR=cache_path)
        self.catalog = cat
        # NOTE: for executor we cannot preload cache as it exceeds memory!
        self.cache = None

    @log_execution_time(_LOG)
    def get_cached_product(
        self, dataset_id: str, product_id: str
    ) -> DataCube | Dataset:
        """Get product from the cache rather than directly loading from
        the catalog. If might be `geokube.DataCube` or `geokube.Dataset`.

        Parameters
        -------
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the dataset

        Returns
        -------
        kube : DataCube or Dataset
            Data stored in the cache (either `geokube.DataCube` or `geokube.Dataset`)
        """
        if self.cache is None:
            self.cache = {}
            self._load_cache()
        if (
            dataset_id not in self.cache
            or product_id not in self.cache[dataset_id]
        ):
            self._LOG.info(
                "dataset `%s` or product `%s` not found in cache! Reading"
                " product!",
                dataset_id,
                product_id,
            )
            self.cache[dataset_id][product_id] = self.catalog[dataset_id][
                product_id
            ].read_chunked()
        return self.cache[dataset_id][product_id]

    @log_execution_time(_LOG)
    def _load_cache(self):
        for i, dataset_id in enumerate(self.dataset_list()):
            self._LOG.info(
                "loading cache for `%s` (%d/%d)",
                dataset_id,
                i + 1,
                len(self.dataset_list()),
            )
            self.cache[dataset_id] = {}
            for product_id in self.product_list(dataset_id):
                try:
                    self.cache[dataset_id][product_id] = self.catalog[
                        dataset_id
                    ][product_id].read_chunked()
                except ValueError:
                    self._LOG.error(
                        "failed to load cache for `%s.%s`",
                        dataset_id,
                        product_id,
                        exc_info=True,
                    )

    @staticmethod
    def _maybe_convert_dict_slice_to_slice(dict_vals):
        if "start" in dict_vals or "stop" in dict_vals:
            return slice(
                dict_vals.get("start"),
                dict_vals.get("stop"),
                dict_vals.get("step"),
            )
        return dict_vals

    @log_execution_time(_LOG)
    def dataset_list(self) -> list:
        """Get list of datasets available in the catalog stored in `catalog`
        attribute

        Returns
        -------
        datasets : list
            List of datasets present in the catalog
        """
        datasets = set(self.catalog)
        datasets -= {
            "medsea-rea-e3r1",
            "medsea-cmip5-projections-biogeochemistry",
        }
        # NOTE: medsae cmip uses cftime.DatetimeNoLeap as time
        # need to think how to handle it
        return sorted(list(datasets))

    @log_execution_time(_LOG)
    def product_list(self, dataset_id: str):
        """Get list of products available in the catalog for dataset
        indicated by `dataset_id`

        Parameters
        ----------
        dataset_id : str
            ID of the dataset

        Returns
        -------
        products : list
            List of products for the dataset
        """
        return list(self.catalog[dataset_id])

    @log_execution_time(_LOG)
    def dataset_info(self, dataset_id: str):
        """Get information about the dataset and names of all available
        products (with their metadata)

        Parameters
        ----------
        dataset_id : str
            ID of the dataset

        Returns
        -------
        info : dict
            Dict of short information about the dataset
        """
        info = {}
        entry = self.catalog[dataset_id]
        if entry.metadata:
            info["metadata"] = entry.metadata
            info["metadata"]["id"] = dataset_id
        info["products"] = {}
        for product_id in self.catalog[dataset_id]:
            entry = self.catalog[dataset_id][product_id]
            info["products"][product_id] = entry.metadata
            info["products"][product_id]["description"] = entry.description
        return info

    @log_execution_time(_LOG)
    def product_metadata(self, dataset_id: str, product_id: str):
        """Get product metadata directly from the catalog.

        Parameters
        ----------
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the product

        Returns
        -------
        metadata : dict
            DatasetMetadata of the product
        """
        return self.catalog[dataset_id][product_id].metadata

    @log_execution_time(_LOG)
    def product_details(
        self, dataset_id: str, product_id: str, use_cache: bool = False
    ):
        """Get details for the single product

        Parameters
        ----------
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the product
        use_cache : bool, optional, default=False
            Data will be loaded from cache if set to `True` or directly
            from the catalog otherwise


        Returns
        -------
        details : dict
            Details of the product
        """
        info = {}
        entry = self.catalog[dataset_id][product_id]
        if entry.metadata:
            info["metadata"] = entry.metadata
        info["description"] = entry.description
        info["id"] = product_id
        info["dataset"] = self.dataset_info(dataset_id=dataset_id)
        if use_cache:
            info["data"] = self.get_cached_product(
                dataset_id, product_id
            ).to_dict()
        else:
            info["data"] = (
                self.catalog[dataset_id][product_id].read_chunked().to_dict()
            )
        return info

    def product_info(
        self, dataset_id: str, product_id: str, use_cache: bool = False
    ):
        info = {}
        entry = self.catalog[dataset_id][product_id]
        if entry.metadata:
            info["metadata"] = entry.metadata
        if use_cache:
            info["data"] = self.get_cached_product(
                dataset_id, product_id
            ).to_dict()
        else:
            info["data"] = (
                self.catalog[dataset_id][product_id].read_chunked().to_dict()
            )
        return info

    @log_execution_time(_LOG)
    def query(
        self,
        dataset_id: str,
        product_id: str,
        query: GeoQuery | dict | str,
        compute: bool = False,
    ) -> DataCube:
        """Query dataset

        Parameters
        ----------
        dataset_id : str
            ID of the dataset
        product_id : str
            ID of the product
        query : GeoQuery or dict or str
            Query to be executed for the given product
        compute : bool, optional, default=False
            If True, resulting data of DataCube will be computed, otherwise
            DataCube with `dask.Delayed` object will be returned


        Returns
        -------
        kube : DataCube
            DataCube processed according to `query`
        """
        if isinstance(query, str):
            query = json.loads(query)
        if isinstance(query, dict):
            query = GeoQuery(**query)
        # NOTE: we always use catalog directly and single product cache
        kube = self.catalog[dataset_id][product_id].read_chunked()
        if isinstance(kube, Dataset):
            kube = kube.filter(**query.filters)
        if query.variable:
            kube = kube[query.variable]
        if query.area:
            kube = kube.geobbox(**query.area)
        if query.locations:
            kube = kube.locations(**query.locations)
        if query.time:
            kube = kube.sel(
                **{
                    "time": Datastore._maybe_convert_dict_slice_to_slice(
                        query.time
                    )
                }
            )
        if query.vertical:
            kube = kube.sel(vertical=query.vertical, method="nearest")
        if compute:
            # FIXME: TypeError: __init__() got an unexpected keyword argument
            # 'fastpath'
            kube.compute()
        return kube
