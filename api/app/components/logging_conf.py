import os
import sys
import logging


_DEFAULT_FORMATTER = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s"
)


def configure_logger(obj):
    if not hasattr(obj, "_LOG"):
        return
    obj._LOG.handlers = []
    logging_level = os.environ.get("LOGGING_LEVEL", logging.WARNING)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(
        os.environ.get("LOGGING_FORMAT", _DEFAULT_FORMATTER)
    )
    stream_handler.setLevel(logging_level)
    obj._LOG.addHandler(stream_handler)
    obj._LOG.setLevel(logging_level)
