from __future__ import annotations

import os
import intake
import json
import logging
import traceback

from geokube.core.datacube import DataCube
from geokube.core.dataset import Dataset
from geoquery.geoquery import GeoQuery

from .singleton import Singleton


class Datastore(metaclass=Singleton):

    _LOG = logging.getLogger("Datastore")

    def __init__(self, cache_path: str = "./") -> None:
        if "CATALOG_PATH" not in os.environ:
            self._LOG.error(
                "Missing required environment variable: 'CATALOG_PATH'"
            )
            raise KeyError(
                "Missing required environment variable: 'CATALOG_PATH'"
            )
        cat = intake.open_catalog(os.environ["CATALOG_PATH"])
        #        self.catalog = cat(CACHE_DIR=cache_path)
        self.catalog = cat
        # NOTE: for executor we cannot preload cache as it exceeds memory!
        self.cache = None

    def get_cached_product(self, dataset_id, product_id):
        if self.cache is None:
            self.cache = {}
            self._load_cache()
        if (
            dataset_id not in self.cache
            or product_id not in self.cache[dataset_id]
        ):
            self._LOG.warning(
                f"Dataset `{dataset_id}` or product `{product_id}` not found"
                " in cache! Reading product!"
            )
            self.cache[dataset_id][product_id] = self.catalog[dataset_id][
                product_id
            ].read_chunked()
        return self.cache[dataset_id][product_id]

    def _load_cache(self):
        for i, dataset_id in enumerate(self.dataset_list()):
            self._LOG.info(
                "Loading cache for"
                f" {dataset_id} ({i+1}/{len(self.dataset_list())})"
            )
            self.cache[dataset_id] = {}
            for product_id in self.product_list(dataset_id):
                try:
                    self.cache[dataset_id][product_id] = self.catalog[
                        dataset_id
                    ][product_id].read_chunked()
                except ValueError as err:
                    self._LOG.error(
                        f"Failed to load cache for `{dataset_id}.{product_id}`"
                        f" due to error: {err}. Traceback:"
                        f" {traceback.format_exc()}"
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

    def dataset_list(self):
        return list(self.catalog)

    def product_list(self, dataset_id: str):
        return list(self.catalog[dataset_id])

    def dataset_info(self, dataset_id: str, use_cache: bool = True):
        info = {}
        entry = self.catalog[dataset_id]
        if entry.metadata:
            info["metadata"] = entry.metadata
        info["products"] = {}
        for p in self.catalog[dataset_id]:
            info["products"][p] = self.product_info(
                dataset_id, product_id, use_cache
            )

    def product_metadata(self, dataset_id: str, product_id: str):
        return self.catalog[dataset_id][product_id].metadata

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

    def query(
        self,
        dataset: str,
        product: str,
        query: GeoQuery | dict | str,
        compute: bool = False,
    ) -> DataCube:
        """
        :param dataset: dataset name
        :param product: product name
        :param query: subset query
        :param path: path to store
        :return: subsetted geokube of selected dataset product
        """
        if isinstance(query, str):
            query = json.loads(query)
        if isinstance(query, dict):
            query = GeoQuery(**query)
        # NOTE: we always use catalog directly and single product cache
        kube = self.catalog[dataset][product].read_chunked()
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
