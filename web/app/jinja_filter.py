from jinja2.runtime import Undefined
import json


def required(input, key):
    if isinstance(input, Undefined):
        raise KeyError(f"Key `{key}` is required!")


def escape_chars(input):
    return json.dumps(input)
