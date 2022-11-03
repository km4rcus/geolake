from __future__ import annotations

from fastapi import HTTPException


class UserCredentials:
    def __init__(self, user_token: None | str):
        if user_token:
            self.__is_public = False
            self.__user_id, self.__user_key = get_user_id_and_key_from_token(
                user_token
            )
            try:
                self.__user_id = self.__user_id
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Token was not provided or it has a wrong format!"
                        f" Correct format is <user_id>:<user_key>."
                    ),
                )
        else:
            self.__is_public = True
            self.__user_id = self.__user_key = None

    @property
    def is_public(self) -> bool:
        return self.__is_public

    @property
    def id(self) -> int:
        return self.__user_id

    @property
    def key(self) -> str:
        return self.__user_key

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


def get_user_id_and_key_from_token(user_token: str):
    if user_token is None:
        raise HTTPException(
            status_code=400,
            detail=f"User token cannot be None",
        )
    splits = list(
        filter(lambda x: x is not None and x != "", user_token.split(":"))
    )
    if ":" not in user_token or len(splits) != 2:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Token was not provided or it has a wrong format! Correct"
                f" format is <user_id>:<user_key>."
            ),
        )
    return user_token.split(":")
