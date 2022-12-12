"""Module contains Context class definition"""
from uuid import UUID, uuid4

from fastapi import Request

from .utils.auth import UserCredentials
from .access import AccessManager
from .exceptions import AuthenticationFailed


class Context:
    """The class managing execution context of the single request passing
    through the Web component. Its attributes are immutable when set to
    non-None values.

    Context contains following attributes:
    1. user: UserCredentials
        Credentials of the user within the context
    2. rid: UUID
        ID of the request passing throught the Web component

    """

    user: UserCredentials
    rid: UUID

    def __init__(
        self,
        request: Request,
        authorization: str = None,
        *,
        enable_public: bool = False
    ):
        """Create an instance of the context.

        Parameters
        ----------
        request : fastapi.Request
            A request object to use
        authorization : optional, str, default=`None`
            Authorization token in the format `Bearer ...`
        enable_public : optional, bool, default=`False`
            Flag indicating if public profile is allowed or if authenticated
            user is required

        Raises
        ------
        AuthenticationFailed
            If authorization token was wrong and `enable_public` was set
            to `False`
        """
        self.user = None
        if authorization:
            try:
                self.define_user(authorization)
            except AuthenticationFailed as err:
                if enable_public:
                    self.user = UserCredentials()
                else:
                    raise err
        self.rid = uuid4()

    def __delattr__(self, name):
        if getattr(self, name, None) is not None:
            raise AttributeError("The attribute '{name}' cannot be deleted!")
        super().__delattr__(name)

    def __setattr__(self, name, value):
        if getattr(self, name, None) is not None:
            raise AttributeError(
                "The attribute '{name}' cannot modified when not None!"
            )
        super().__setattr__(name, value)

    def is_user_defined(self):
        """Check if user is defined for the context

        Returns
        -------
        user_defined : bool
            `True` if user is defined for the context, `False` otherwise
        """
        return self.user is not None

    def define_user(self, authorization: str):
        """Define the user for the context by means of the 'authorization' header.
        If 'authorization' is 'None', defines the public profile.

        Parameters
        ----------
        authorization : str
            Authorization header

        Raises
        ------
        AuthenticationFailed
            if authorization token was not associated with the user
        """
        self.user = AccessManager.retrieve_credentials_from_jwt(authorization)
