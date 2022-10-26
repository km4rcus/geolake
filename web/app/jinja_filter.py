import json
from jinja2.runtime import Undefined


def required(load, key):
    if isinstance(load, Undefined):
        raise KeyError(f"Key `{key}` is required!")
    return load


def escape_chars(load):
    return json.dumps(load)
