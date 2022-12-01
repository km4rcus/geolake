"""Module with utils for number handling"""
from numbers import Number


def maybe_round_value(item, decimals=2):
    """Round a number to take number decimal digits indicated by 'decimals' argument.
    It 'item' is not float or int, returns original value

    Parameters
    ----------
    item : Any
        Item to be rounded (if number)
    decimals : int, default=2
        Number of decimal places

    Returns
    -------
    item : Any
        Rounded value (if number) or original value otherwise
    """
    return round(item, decimals) if isinstance(item, Number) else item


def prepare_estimate_size_message(
    maximum_allowed_size_gb: float, estimated_size_gb: float
):
    """Prepare estimate size and maximum allowed size into the message
        expected by the Webportal
    Parameters
    ----------
    maximum_allowed_size_gb : float
        Maximum allowed size in gigabytes
    estimated_size_gb : float
        Estimated size in gigabytes
    Returns
    -------
    message : dict
        A dicitonary with keys `status` and `message`
    """
    status = "OK"
    if estimated_size_gb is None:
        status = "Error"
        msg = "Could not estimate the size for that dataset"
    if estimated_size_gb > maximum_allowed_size_gb:
        status = "Error"
        msg = (
            f"Estimated request size ({estimated_size_gb} GB) is more than"
            f" maximum allowed size ({maximum_allowed_size_gb} GB). Please"
            " review your query"
        )
    else:
        msg = f"Estimated request size: {estimated_size_gb} GB"
    return {"status": status, "message": msg}
