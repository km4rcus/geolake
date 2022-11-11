"""Utils module for geokube-dds API component"""
from __future__ import annotations


from uuid import UUID
from fastapi import HTTPException


class UserCredentials:
    """Class containing current user credentials"""

    __slots__ = ("_is_public", "_user_id", "_user_key")

    def __init__(self, user_token: None | str):
        if user_token:
            self._is_public = False
            self._user_id, self._user_key = _get_user_id_and_key_from_token(
                user_token
            )
        else:
            self._is_public = True
            self._user_id = self._user_key = None

    @property
    def is_public(self) -> bool:
        """Determine if the current user is public (anonymous)"""
        return self._is_public

    @property
    def id(self) -> int:
        """Get the ID of the current user"""
        return self._user_id

    @property
    def key(self) -> str:
        "Get key of the current user"
        return self._user_key

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


def _get_user_id_and_key_from_token(user_token: str):
    if user_token is None:
        raise HTTPException(
            status_code=400,
            detail="User token cannot be None",
        )
    if ":" not in user_token:
        raise HTTPException(
            status_code=400,
            detail="User token must be in the format <user_id>:<api_key>!",
        )
    user_id, api_key, *rest = user_token.split(":")
    if len(rest) > 0:
        raise HTTPException(
            status_code=400,
            detail="User token must be in the format <user_id>:<api_key>!",
        )
    try:
        _ = UUID(user_id, version=4)
    except ValueError as err:
        raise HTTPException(
            status_code=400,
            detail="User token must be in the UUID4 fromat!",
        ) from err
    else:
        return (user_id, api_key)
