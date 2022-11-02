"""Module with `LoggableMeta` metaclass"""
import os
import logging


class LoggableMeta(type):
    """Metaclass for dealing with logger levels and handlers"""

    def __new__(cls, child_cls, bases, namespace):
        # NOTE: method is called while creating a class, not an instance!
        # TODO: eventually, configure logging format
        res = super().__new__(cls, child_cls, bases, namespace)
        if hasattr(res, "_LOG"):
            res._LOG.setLevel(os.environ.get("LOGGING_LEVEL", "INFO"))
            res._LOG.addHandler(logging.StreamHandler())
        return res
