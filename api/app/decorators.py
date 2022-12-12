from functools import wraps
from inspect import signature, Signature
import logging
import time

from .context import Context
from .utils import dataset as dut


def _assert_parameters_are_defined(
    sig: Signature, required_parameters: list[tuple]
):
    for param_name, param_type in required_parameters:
        if (
            param_name not in sig.parameters
            or sig.parameters[param_name].annotation != param_type
        ):
            raise ValueError(
                f"annotation: {sig.parameters['context'].annotation} ||"
                f" expected type: {param_type} ||"
                f" {type(sig.parameters['context'].annotation)}"
            )
            raise TypeError(
                f"The parameter '{param_name}' annotated with the type"
                f" '{param_type}' must be defined for the callable decorated"
                " with 'authenticate_user' decorator"
            )


def _bind_arguments(sig: Signature, *args, **kwargs):
    args_bind = sig.bind_partial(*args, **kwargs)
    args_bind.apply_defaults()
    return args_bind.arguments


def authenticate(enable_public: bool = False):
    """Decorator for convenient authentication management"""

    def do_authentication(func):
        sig = signature(func)
        _assert_parameters_are_defined(
            sig, required_parameters=[("context", Context)]
        )

        @wraps(func)
        def wrapper_sync(*args, **kwargs):
            args_dict = _bind_arguments(sig, *args, **kwargs)
            context = args_dict["context"]
            if not enable_public:
                context.assert_not_public()
            context.authenticate()
            return func(*args, **kwargs)

        return wrapper_sync

    return do_authentication


def assert_product_exists(func):
    """Decorator for convenient checking if product is defined in the catalog"""
    sig = signature(func)
    _assert_parameters_are_defined(
        sig, required_parameters=[("dataset_id", str), ("product_id", str)]
    )

    @wraps(func)
    def assert_inner(*args, **kwargs):
        args_dict = _bind_arguments(sig, *args, **kwargs)
        dataset_id = args_dict["dataset_id"]
        product_id = args_dict["product_id"]
        dut.assert_product_exists(dataset_id, product_id)
        return func(*args, **kwargs)

    return assert_inner


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
                    "execution of '%s' function from '%s' package took"
                    " %.4f sec",
                    func.__name__,
                    func.__module__,
                    time.monotonic() - exec_start_time,
                )

        return wrapper

    return inner
