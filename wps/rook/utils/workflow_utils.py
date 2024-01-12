import re
import urllib3
from rook import CONFIG

_MONTHS_MAPPING = {
    'jan': '1', 'feb': '2', 'mar': '3', 'apr': '4', 
    'may': '5', 'jun': '6', 'jul': '7', 'aug': '8', 
    'sep': '9', 'oct': '10', 'nov': '11', 'dec': '12'
}

_FREQ_MAPPING = {
    "day": "1D",
    "month": "1M",
    "year": "1Y",
}

def run_workflow(args):
    import ddsapi       
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print(f'ARGS:{args}')
    # args = {'collection': ['cordex.adjusted.pr.CNRM-CERFACS-CNRM-CM5.45.r1i1p1.CLMcom-CCLM4-8-17.v1-IPSL-CDFT22s-MESAN-1989-2005.day'], 'steps': {'subset_pr_1': {'run': 'subset', 'in': {'collection': ['cordex.adjusted.pr.CNRM-CERFACS-CNRM-CM5.45.r1i1p1.CLMcom-CCLM4-8-17.v1-IPSL-CDFT22s-MESAN-1989-2005.day'], 'time': '1951-01-01/1958-12-31', 'apply_fixes': False, 'api_key': 'e1f21178-14dc-47c0-be72-203b97c1e344:c9diEIbi5iEGbN2FSgO-Mx9W4NlNZXlRQmUpStiDkgU'}}, 'subset_pr_2': {'run': 'subset', 'in': {'collection': ['subset_pr_1_output'], 'time': '1953-01-01/1955-12-31', 'apply_fixes': False, 'api_key': 'e1f21178-14dc-47c0-be72-203b97c1e344:c9diEIbi5iEGbN2FSgO-Mx9W4NlNZXlRQmUpStiDkgU'}}}, 'output_dir': '/tmp/pywps_process_o3c01zqu/workflow_operations_sqz07ktv', 'apply_fixes': False}
    url = CONFIG.get("ddsapi").get("url")
    api_key = args.get("steps")[next(iter(args.get("steps")))]["in"]["api_key"]
    c = ddsapi.Client(url=url, key=api_key)

    tasks = []
    output_dir = args.get("output_dir")
    collection = args.get("collection")[0]
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

    if dataset_id == 'CMIP6':
        dataset_id = dataset_id.lower()
        fltrs_dict.pop('version')

    for step in args["steps"]:
        op = args["steps"][step]["run"]
        if op == 'average_time':
            op = 'resample'
        task = {"id": step, "op": op, "args": {}}
        if step == list(args["steps"])[0]:
            task["args"]["dataset_id"] = dataset_id
            task["args"]["product_id"] = product_id
            task["args"]["query"] = fltrs_dict
        else:
            used_item = args["steps"][step]["in"]["collection"][0].rsplit('_', 1)[0]
            task["use"] = [used_item]
        options = args["steps"][step]["in"]
        if "area" in options:
            area_vals = options["area"].split(',')
            west = float(area_vals[0])
            east = float(area_vals[2])
            if float(area_vals[1]) > float(area_vals[3]):
                north = float(area_vals[1])
                south = float(area_vals[3])
            else:
                north = float(area_vals[3])
                south = float(area_vals[1])
            task["args"]["query"]["area"] =  {
                "north": north,
                "south": south,
                "east": east,
                "west": west
            }
        if "time" in options:
            time_output = (((options["time"]).replace("-", "")).replace("/", "-")).replace(" ", "")
            if '/' in options["time"]:
                start_date = options["time"].split('/')[0]
                end_date = options["time"].split('/')[1]
                task["args"]["query"]["time"] = {
                    "start": start_date,
                    "stop": end_date
                }
                
            else:
                time_vals = options["time"].replace(" ", "").split(',')
        if "time_components" in options:
            time_combo = dict(x.split(":") for x in options["time_components"].split("|"))
            for k, v in time_combo.items():
                if k == 'year':
                    time_combo['year'] = list(map(str, v.split(',')))
                elif k == 'month':
                    months = v.split(',')
                    time_combo['month'] = [_MONTHS_MAPPING.get(item,item) for item in months]
                elif k == 'day':
                    time_combo['day'] = list(map(str, v.split(',')))
            task["args"]["query"]["time"] = time_combo
        if "freq" in options:
            task["args"]["freq"] = _FREQ_MAPPING.get(options["freq"])
            task["args"]["agg"] = 'mean'
            task["args"]["resample_kwargs"] = {}
        if "dims" in options:
            task["args"]["dim"] = options["dims"]
            # raise NotImplementedError("Average operation is not available")
        tasks.append(task)
    request = {"tasks": tasks}
    tmp_output_path = f'{output_dir}/{collection.replace(".", "_")}_{time_output}.nc'
    print(request)
    c.retrieve(request, tmp_output_path)
    file_uris = [tmp_output_path]
    return file_uris