import re
import urllib3
from rook import CONFIG

_FREQ_MAPPING = {
    "day": "1D",
    "month": "1M",
    "year": "1Y",
}

def elab_request_filters(collection):
    query = {}
    params = collection.split('.')
    dataset_id = params[0]
    if dataset_id == 'CMIP6':
        product_id = 'scenario'
        fltrs_vals = params[1:] 
        fltrs = CONFIG.get(f"project:{dataset_id}").get("pattern")
        fltrs_keys = (re.findall('{(.+?)}',fltrs))[1:]
    else:
        product_id = params[1]
        fltrs_vals = params[2:] 
        fltrs = CONFIG.get(f"project:{dataset_id}.{product_id}").get("pattern")
        fltrs_keys = (re.findall('{(.+?)}',fltrs))[2:]

    fltrs_dict = dict(zip(fltrs_keys, fltrs_vals))
    query.update(fltrs_dict)

    if dataset_id == 'CMIP6':
        dataset_id = dataset_id.lower()
        query.pop('version')
    return dataset_id, product_id, query


def run_average_by_time(args):
    import ddsapi       
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print(f'ARGS:{args}')
    url = CONFIG.get("ddsapi").get("url")
    api_key = args.get("api_key")
    freq = args.get("freq")
    output_dir = args.get("output_dir")

    c = ddsapi.Client(url=url, key=api_key)

    collection = args.get("collection")[0].split('.dds')[0]
    tmp_output_path = f'{output_dir}/{collection.replace(".", "_")}_{freq}_resample.nc'
    dataset_id, product_id, query = elab_request_filters(collection)
    
    request = { 
        "tasks": [
        {
            "id": "prim0",
            "op": "subset",
            "args":{
                "dataset_id": dataset_id,
                "product_id": product_id,
                "query": query
            }
        },
        {
            "id": "prim1",
            "op": "resample",
            "use": ["prim0"],
            "args": {
                "freq": _FREQ_MAPPING.get(freq),
                "agg": "mean",
                "resample_kwargs": {}
            },
        }]
    }
    print(request)
    c.retrieve(request, tmp_output_path)

    file_uris = [tmp_output_path]
    return file_uris


def run_average_by_dim(args):
    import ddsapi       
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print(f'ARGS:{args}')
    url = CONFIG.get("ddsapi").get("url")
    api_key = args.get("api_key")
    dim = args.get("dims")[0]
    output_dir = args.get("output_dir")

    c = ddsapi.Client(url=url, key=api_key)

    collection = args.get("collection")[0].split('.dds')[0]
    tmp_output_path = f'{output_dir}/{collection.replace(".", "_")}_{dim}_average.nc'
    dataset_id, product_id, query = elab_request_filters(collection)
    
    request = { 
        "tasks": [
        {
            "id": "prim0",
            "op": "subset",
            "args":{
                "dataset_id": dataset_id,
                "product_id": product_id,
                "query": query
            }
        },
        {
            "id": "prim1",
            "op": "average",
            "use": ["prim0"],
            "args": {
                "dim": dim,
            },
        }]
    }
    print(request)
    c.retrieve(request, tmp_output_path)
    file_uris = [tmp_output_path]
    # raise NotImplementedError("Average operation is not available")
    return file_uris
