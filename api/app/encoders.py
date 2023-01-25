import numpy as np
from fastapi.encoders import encoders_by_class_tuples


def make_ndarray_dtypes_valid(o: np.ndarray) -> np.ndarray:
    """Convert `numpy.array` dtype to the one which is serializable
    to JSON.

    int32 -> int64
    float32 -> float 64

    Parameters
    ----------
    o : np.ndarray
        A NumPy array object

    Returns
    -------
    res : np.ndarray
        A NumPy array object with dtype set properly

    Raises
    ------
    AssertionError
        If passed object is not of `numpy.ndarray`
    """
    assert isinstance(o, np.ndarray)
    if np.issubdtype(o.dtype, np.int32):
        return o.astype(np.int64)
    if np.issubdtype(o.dtype, np.float32):
        return o.astype(np.float64)


def extend_json_encoders():
    """Extend `encoders_by_class_tuples` module variable from `fastapi.encoders`
    with auxiliary encoders necessary for proper application working."""
    encoders_by_class_tuples[lambda o: list(make_ndarray_dtypes_valid(o))] = (
        np.ndarray,
    )
    encoders_by_class_tuples[str] += (np.int32, np.float32)
