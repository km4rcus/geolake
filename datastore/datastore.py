import intake
from geokube.core.datacube import DataCube
from geokube.core.dataset import Dataset
from typing import Union
from geoquery.geoquery import GeoQuery
import json

class Datastore():

    def __init__(self, cat_path: str) -> None:
        self.catalog = intake.open_catalog(cat_path)
    
    def dataset_list(self):
        return list(self.catalog)

    def product_list(self, dataset_id: str):
        return list(self.catalog[dataset_id])

    def dataset_info(self, dataset_id: str):
        info = {}
        entry = self.catalog[dataset_id]
        if entry.metadata:
            info['metadata'] = entry.metadata
        info['products'] = {}
        for p in self.products():
            info['products'][p] = self.product_info()

    def product_info(self, dataset_id: str, product_id: str):
        info = {}
        entry = self.catalog[dataset_id][product_id]
        if entry.metadata:
            info['metadata'] = entry.metadata
        info.update(entry.read_chunked().to_dict())
        return info    

    def query(self, dataset: str, product: str, query: Union[GeoQuery, dict, str], compute: bool=False):
        """
        :param dataset: dasaset name
        :param product: product name
        :param query: subset query
        :param path: path to store
        :return: subsetted hypycube of selected dataset product
        """
        if isinstance(query, str):
            query = json.loads(query)
        if isinstance(query, dict):
            query = GeoQuery(**query)
        kube = self.catalog[dataset][product].read_chunked()
        if isinstance(kube, Dataset):
            kube = kube.filter(query.filters)
        if query.variable:
            kube = kube[query.variable]
        if query.area:
            kube = kube.geobbox(query.area)
        if query.locations:
            kube = kube.locations(**query.locations)
        if query.time:
            kube = kube.sel(query.time)
        if query.vertical:
            kube = kube.sel(query.vertical)
        if compute:
            kube.compute()
        return kube