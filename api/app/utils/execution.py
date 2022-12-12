"""Module with execution utils"""
from functools import wraps
import time
import logging


def log_execution_time(logger: logging.Logger):
    """Decorator logging execution time of the method or function"""

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwds):
            exec_start_time = time.monotonic()
            try:
                return func(*args, **kwds)
            finally:
                # NOTE: maybe logging should be on DEBUG level
                logger.info(
                    "execution of '%s' function from '%s' package took %s",
                    func.__name__,
                    func.__module__,
                    time.monotonic() - exec_start_time,
                )

        return wrapper

    return inner
