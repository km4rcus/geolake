import time
import logging as default_logging
from functools import wraps
from typing import Literal


def log_execution_time(
    logger: default_logging.Logger,
    level: Literal["debug", "info", "warning", "error", "critical"] = "info",
):
    """Decorator logging execution time of the method or function"""
    level = default_logging.getLevelName(level.upper())

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwds):
            exec_start_time = time.monotonic()
            try:
                return func(*args, **kwds)
            finally:
                # NOTE: maybe logging should be on DEBUG level
                logger.log(
                    level,
                    "execution of '%s' function from '%s' package took"
                    " %.4f sec",
                    func.__name__,
                    func.__module__,
                    time.monotonic() - exec_start_time,
                )

        return wrapper

    return inner
