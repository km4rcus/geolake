from __future__ import annotations

from fastapi import HTTPException


class UserCredentials:
    def __init__(self, user_token: None | str):
        if user_token:
            self.__is_public = False
            self.__user_id, self.__user_key = get_user_id_and_key_from_token(
                user_token
            )
            self.__user_id = int(self.__user_id)
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


def get_user_id_and_key_from_token(user_token: str):
    if user_token is None or ":" not in user_token:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Token was not provided or it has a wrong format! Correct"
                f" format is <user_id>:<user_key>."
            ),
        )
    return user_token.split(":")
