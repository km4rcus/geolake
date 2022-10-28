"""Custom Jinja2 filters"""
from functools import wraps
import json
import warnings
from jinja2.runtime import Undefined
from jinja2.filters import FILTERS


def register_filter(func):
    """Decorator registering the given function `func` as Jinja2 filter
    with the name indicated in by the function `__name__` attribute"""

    @wraps(func)
    def wrapper(*args, **kwds):
        return func(*args, **kwds)

    if func.__name__ in FILTERS:
        warnings.warn(
            f"filter with the name `{func.__name__}` is already registered in"
            " Jinja2 Filters - it will be overwritten!"
        )
    FILTERS[func.__name__] = func
    return wrapper


@register_filter
def required(load, key):
    """Require key to be specified"""
    if isinstance(load, Undefined):
        raise KeyError(f"Key `{key}` is required!")
    return load


@register_filter
def escape_chars(load):
    """Escape characters by converting to JSON"""
    return json.dumps(load)
