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
