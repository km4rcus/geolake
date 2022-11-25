"""Module with `LoggableMeta` metaclass"""
import os
import logging


class LoggableMeta(type):
    """Metaclass for dealing with logger levels and handlers"""

    def __new__(cls, child_cls, bases, namespace):
        # NOTE: method is called while creating a class, not an instance!
        res = super().__new__(cls, child_cls, bases, namespace)
        if hasattr(res, "_LOG"):
            format_ = os.environ.get(
                "LOGGING_FORMAT",
                "%(asctime)s %(name)s %(levelname)s %(lineno)d %(track_id)s"
                " %(message)s",
            )
            formatter = logging.Formatter(format_)
            res._LOG.setLevel(os.environ.get("LOGGING_LEVEL", "INFO"))
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            res._LOG.addHandler(stream_handler)
        return res
