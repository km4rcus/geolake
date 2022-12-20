"""Utils module"""
from typing import Literal


def convert_bytes(size_bytes: int, to: Literal["kb", "mb", "gb"]) -> float:
    """Converts size in bytes to the other unit - one out of:
    ["kb", "mb", "gb"]

    Parameters
    ----------
    size_bytes : int
        Size in bytes
    to : str
        Unit to convert `size_bytes` to

    size : float
        `size_bytes` converted to the given unit
    """
    to = to.lower()
    if to == "kb":
        value = size_bytes / 1024
    elif to == "mb":
        value = size_bytes / 1024**2
    elif to == "gb":
        value = size_bytes / 1024**3
    else:
        raise ValueError(f"unsupported units: {to}")
    return value


def make_bytes_readable_dict(size_bytes: int, units: str = None) -> dict:
    """Prepare dictionary representing size (in bytes) in more readable unit
    to keep value in the range [0,1] - if `units` is `None`.
    If `units` is not None, converts `size_bytes` to the size expressed by
    that argument.

    Parameters
    ----------
    size_bytes : int
        Size expressed in bytes
    units : optional, str
        Units (case insensitive), one of [kB, MB, GB]
    Returns
    -------
    result : dict
        A dictionary with size and units in the form:
        {
            "value": ...,
            "units": ...
        }
    """
    if units is None:
        units = "bytes"
    if units != "bytes":
        size_bytes = convert_bytes(size_bytes=size_bytes, to=units)
        return {"value": size_bytes, "units": units}
    val = size_bytes
    if val > 1024:
        units = "kB"
        val /= 1024
    if val > 1024:
        units = "MB"
        val /= 1024
    if val > 1024:
        units = "GB"
        val /= 1024
    if val > 0.0 and (val := round(val, 2)) == 0.00:
        val = 0.01
    return {"value": val, "units": units}
