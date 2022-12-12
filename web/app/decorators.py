from functools import wraps

from inspect import signature, Signature, iscoroutinefunction
from fastapi.params import Header


from .context import Context
from .exceptions import AuthenticationFailed
from .access import AccessManager
from .utils.auth import UserCredentials


def assert_context_parameter_is_defined(sig: Signature):
    if (
        "context" not in sig.parameters
        or sig.parameters["context"].annotation != Context
    ):
        raise TypeError(
            "The parameter 'context' annotated with the type"
            " <context.Context> must be defined for the callable decorated"
            " with 'authenticate_user' decorator"
        )


def bind_arguments(sig: Signature, *args, **kwargs):
    args_bind = sig.bind_partial(*args, **kwargs)
    args_bind.apply_defaults()
    return args_bind.arguments


def authenticate_user(enable_public: bool = False):
    def do_auth(func):
        sig = signature(func)
        assert_context_parameter_is_defined(sig)

        @wraps(func)
        async def async_inner(*args, **kwargs):
            args_dict = bind_arguments(sig, *args, **kwargs)
            context = args_dict["context"]
            user_cred = UserCredentials()
            try:
                user_cred = AccessManager.retrieve_credentials_from_jwt(
                    args_dict.get(["authorization"])
                )
            except AuthenticationFailed as err:
                if not enable_public:
                    raise err
            finally:
                if context.is_user_defined:
                    raise RuntimeError(
                        "user is already defined in the context!"
                    )
                context.define_user(user_cred)

            return await func(context=context, **args_dict)

        @wraps(func)
        def sync_inner(*args, **kwargs):
            args_dict = bind_arguments(sig, *args, **kwargs)
            try:
                user_cred = AccessManager.retrieve_credentials_from_jwt(
                    args_dict.get(["authorization"])
                )
            except AuthenticationFailed as err:
                if not enable_public:
                    raise err
            finally:
                if context.is_user_defined:
                    raise RuntimeError(
                        "user is already defined in the context!"
                    )
                context.define_user(user_cred)

            return func(context=context, **args_dict)

        return async_inner if iscoroutinefunction(func) else sync_inner

    return do_auth
