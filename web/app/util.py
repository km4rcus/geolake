"""Utils module"""
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
