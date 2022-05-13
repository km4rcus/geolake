import pytest
from fastapi import HTTPException

from app.util import UserCredentials


def test_public_user_if_token_none():
    header = None
    ucred = UserCredentials(header)
    assert ucred.id is None
    assert ucred.key is None
    assert ucred.is_public


def test_not_public_if_token_provided():
    header = "1:2"
    ucred = UserCredentials(header)
    assert not ucred.is_public
    assert ucred.id == 1
    assert ucred.key == "2"


def test_token_parse_fail_if_wrong_format():
    header = "1-1"
    with pytest.raises(HTTPException):
        _ = UserCredentials(header)

    header = ":1"
    with pytest.raises(HTTPException):
        _ = UserCredentials(header)

    header = "1:"
    with pytest.raises(HTTPException):
        _ = UserCredentials(header)
