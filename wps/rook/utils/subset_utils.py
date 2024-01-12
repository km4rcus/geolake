import re
import urllib3
from rook import CONFIG
from roocs_utils.utils.file_utils import FileMapper

_MONTHS_MAPPING = {
    'jan': '1', 'feb': '2', 'mar': '3', 'apr': '4', 
    'may': '5', 'jun': '6', 'jul': '7', 'aug': '8', 
    'sep': '9', 'oct': '10', 'nov': '11', 'dec': '12'
}

def run_subset(args):
    import ddsapi       
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print(f'ARGS:{args}')
    url = CONFIG.get("ddsapi").get("url")
    api_key = args.get("api_key")
    area = args.get("area")
    time = args.get("time")
    time_comps = args.get("time_components")
    time_output = (((time).replace("-", "")).replace("/", "-")).replace(" ", "")
    output_dir = args.get("output_dir")

    c = ddsapi.Client(url=url, key=api_key)
    query = {}
    
    #if isinstance(args.get("collection"), FileMapper):
        #tmp_output_path = f'{output_dir}/{(args.get("collection").file_list[0]).replace(".nc", "")}_{time_output}.nc'
        #collection = 'cordex.adjusted.pr.CNRM-CERFACS-CNRM-CM5.45.r1i1p1.CLMcom-CCLM4-8-17.v1-IPSL-CDFT22s-MESAN-1989-2005.day'
    #else:
    collection = args.get("collection")[0].split('.dds')[0]
    tmp_output_path = f'{output_dir}/{collection.replace(".", "_")}_{time_output}.nc'

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

    # lat: north-south - lon: west-east
    # area='0.,49.,10.,65' west-north-east-south
    if area:
        area_vals = area.split(',')
        west = float(area_vals[0])
        east = float(area_vals[2])
        if float(area_vals[1]) > float(area_vals[3]):
            north = float(area_vals[1])
            south = float(area_vals[3])
        else:
            north = float(area_vals[3])
            south = float(area_vals[1])

        query["area"] =  {
            "north": north,
            "south": south,
            "east": east,
            "west": west
        }
    
    if time:
        if '/' in time:
            start_date = time.split('/')[0]
            end_date = time.split('/')[1]
            query["time"] = {
            "start": start_date,
            "stop": end_date
        }
            
        else:
            time_vals = time.replace(" ", "").split(',')
    
    # time_components='year:2016,2017|month:jan,feb,mar|day:01',
    if time_comps:
        time_combo = dict(x.split(":") for x in time_comps.split("|"))
        for k, v in time_combo.items():
            if k == 'year':
                time_combo['year'] = list(map(str, v.split(',')))
            elif k == 'month':
                months = v.split(',')
                time_combo['month'] = [_MONTHS_MAPPING.get(item,item) for item in months]
            elif k == 'day':
                time_combo['day'] = list(map(str, v.split(',')))
        query["time"] = time_combo

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
        ]
    }
    print(request)
    c.retrieve(request, tmp_output_path)
    file_uris = [tmp_output_path]
    return file_uris
