"""Module with execution utils"""
from inspect import iscoroutinefunction
from functools import wraps
import time
import logging


def log_execution_time(logger: logging.Logger):
    """Decorator logging execution time of the method or function (both sync and async)
    """

    def inner(func):
        @wraps(func)
        def wrapper_sync(*args, **kwds):
            exec_start_time = time.monotonic()
            try:
                return func(*args, **kwds)
            finally:
                # NOTE: maybe logging should be on DEBUG level
                logger.info(
                    "execution of '%s' function from '%s' package took %.4f"
                    " seconds",
                    func.__name__,
                    func.__module__,
                    time.monotonic() - exec_start_time,
                )

        @wraps(func)
        async def wrapper_async(*args, **kwds):
            exec_start_time = time.monotonic()
            try:
                return await func(*args, **kwds)
            finally:
                # NOTE: maybe logging should be on DEBUG level
                logger.info(
                    "execution of '%s' function from '%s' package took %.4f"
                    " seconds",
                    func.__name__,
                    func.__module__,
                    time.monotonic() - exec_start_time,
                )

        return wrapper_async if iscoroutinefunction(func) else wrapper_sync

    return inner
