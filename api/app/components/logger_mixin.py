import os
import sys
import logging


class LoggerMixin:

    _DEFAULT_FORMATTER = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    @classmethod
    def configure_logger(cls):
        cls._LOG.handlers = []
        logging_level = os.environ.get("LOGGING_LEVEL", logging.WARNING)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(
            os.environ.get("LOGGING_FORMAT", cls._DEFAULT_FORMATTER)
        )
        stream_handler.setLevel(logging_level)
        cls._LOG.addHandler(stream_handler)
        cls._LOG.setLevel(logging_level)
