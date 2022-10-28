"""Utils module"""
from functools import wraps
import datetime
import logging

from typing import Optional
from pydantic import BaseModel, UUID4


class UserCredentials(BaseModel):
    """Class containing current user credentials, including ID and token"""

    user_id: Optional[UUID4] = None
    user_token: Optional[str] = None

    @property
    def is_public(self) -> bool:
        """Get information if a user uses public profile

        Returns
        -------
        public_flag : bool
            `True` if user uses public profile, `False` otherwise
        """
        return self.user_id is None

    @property
    def id(self) -> UUID4:
        """Get user ID.

        Returns
        -------
        user_id : UUID
            User ID
        """
        return self.user_id

    @property
    def key(self) -> str:
        """Get user API token.

        Returns
        -------
        user_token : str
            User API token
        """
        return self.user_token

    def __eq__(self, other):
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


def log_execution_time(logger: logging.Logger):
    """Decorator logging execution time of the method or function"""

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwds):
            exec_start_time = datetime.datetime.now()
            try:
                return func(*args, **kwds)
            finally:
                exec_time = datetime.datetime.now() - exec_start_time
                logger.info(
                    "execution of `%s` function took %s",
                    func.__name__,
                    exec_time,
                )

        return wrapper

    return inner
