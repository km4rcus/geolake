"""Module contains Context class definition"""
from uuid import UUID

from db.dbmanager.dbmanager import DBManager

from .utils.auth import UserCredentials
from .exceptions import AuthorizationFailed, AuthenticationFailed


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
        rid: str,
        user_token: str,
    ):
        """Create an instance of the context.

        Parameters
        ----------
        request : fastapi.Request
            A request object to use
        rid : str
            Http request id as UUID v4 string
        user_token : str
            User token in the format `<user_id>:<user_key>`

        Raises
        ------
        AuthenticationFailed
            If authorization token was wrong and `enable_public` was set
            to `False`
        """
        self.user = None
        self.define_user(user_token)
        self.rid = rid

    def assert_not_public(self):
        """Assert the user is not public

        Raises
        ------
        AuthorizationFailed
            If user is anonymous
        """
        if self.user.is_public:
            raise AuthorizationFailed(
                "anonymous user is not eligible for that operation"
            )

    def authenticate(self):
        """Authenticate user in the given context

        Raises
        -------
        AuthenticationFailed
        """
        if self.user.is_public:
            return
        user = DBManager().get_user_details(self.user.id)
        if user is None:
            raise AuthenticationFailed
        if user.api_key != self.user.key:
            raise AuthenticationFailed

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

    def define_user(self, user_token: str):
        """Define the user for the context by means of the 'User-Token' header.
        If 'authorization' is 'None', defines the public profile.

        Parameters
        ----------
        user_token : str
            User-Token value

        Raises
        ------
        AuthenticationFailed
            if authorization token was not associated with the user
        """
        self.user = UserCredentials(user_token)
