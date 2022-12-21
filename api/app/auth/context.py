"""Module with auth utils"""
from uuid import UUID
from typing import Optional

from fastapi import Request
from db.dbmanager.dbmanager import DBManager

from ..api_logging import get_dds_logger
from .. import exceptions as exc

log = get_dds_logger(__name__)


class UserCredentials:
    """Class containing current user credentials"""

    __slots__ = ("_user_id", "_user_key")

    def __init__(
        self, user_id: Optional[str] = None, user_key: Optional[str] = None
    ):
        self._user_id = user_id
        if self._user_id is None:
            self._user_key = None
        else:
            self._user_key = user_key

    @property
    def is_public(self) -> bool:
        """Determine if the current user is public (anonymous)"""
        return self._user_id is None

    @property
    def id(self) -> int:
        """Get the ID of the current user"""
        return self._user_id

    @property
    def key(self) -> str:
        "Get key of the current user"
        return self._user_key

    def __eq__(self, other) -> bool:
        if not isinstance(other, UserCredentials):
            return False
        if self.id == other.id and self.key == other.key:
            return True
        return False

    def __ne__(self, other):
        return self != other

    def __repr__(self):
        return (
            f"<UserCredentials(id={self.id}, key=***,"
            f" is_public={self.is_public}>"
        )


class Context:
    """The class managing execution context of the single request passing
    through the Web component. Its attributes are immutable when set to
    non-None values.

    Context contains following attributes:
    1. user: UserCredentials
        Credentials of the user within the context
    2. rid: UUID- like string
        ID of the request passing throught the Web component

    """

    __slots__ = ("rid", "user")

    rid: str
    user: UserCredentials

    def __init__(self, rid: str, user: UserCredentials):
        log.debug("creating new context", extra={"rid": rid})
        self.rid = rid
        self.user = user

    @property
    def is_public(self) -> bool:
        """Determine if the context contains an anonymous user"""
        return self.user.is_public

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


class ContextCreator:
    """Class managing the Context creation"""

    @staticmethod
    def new_context(
        request: Request, *, rid: str, user_token: Optional[str] = None
    ) -> Context:
        """Create a brand new `Context` object based on the provided
        `request`, `rid`, and `user_token` arguments.

        Parameters
        ----------
        request : fastapi.Request
            A request for which context is about to be created
        rid : str
            ID of the DDS Request
        user_token : str
            Token of a user

        Returns
        -------
        context : Context
            A new context

        Raises
        ------
        ImproperUserTokenError
            If user token is not in the right format
        AuthenticationFailed
            If provided api key does not agree with the one stored in the DB
        """
        assert rid is not None, "DDS Request ID cannot be `None`!"
        try:
            user_credentials = UserCredentials(
                *ContextCreator._get_user_id_and_key_from_token(user_token)
            )
        except exc.EmptyUserTokenError:
            # NOTE: we then consider a user as anonymous
            user_credentials = UserCredentials()
        if not user_credentials.is_public:
            log.debug("context authentication", extra={"rid": rid})
            ContextCreator.authenticate(user_credentials)
        context = Context(rid=rid, user=user_credentials)
        return context

    @staticmethod
    def authenticate(user: UserCredentials):
        """Authenticate user. Verify that the provided api agrees with
        the one stored in the database.

        Parameters
        ----------
        user : UserCredentials

        Raises
        ------
        AuthenticationFailed
            If user with the given ID is found in the database but stored api key
            is different than the provided one.
        """
        user = DBManager().get_user_details(user.id)
        if user.api_key != user.key:
            raise exc.AuthenticationFailed(user.id)

    @staticmethod
    def _get_user_id_and_key_from_token(user_token: str):
        if user_token is None or user_token.trim() == "":
            raise exc.EmptyUserTokenError
        if ":" not in user_token:
            raise exc.ImproperUserTokenError
        user_id, api_key, *rest = user_token.split(":")
        if len(rest) > 0:
            raise exc.ImproperUserTokenError
        try:
            _ = UUID(user_id, version=4)
        except ValueError as err:
            raise exc.ImproperUserTokenError from err
        else:
            return (user_id, api_key)
