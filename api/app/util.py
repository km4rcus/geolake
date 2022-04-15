from fastapi import HTTPException


def get_user_id_and_key_from_token(user_token: str):
    if user_token is None or ":" not in user_token:
        raise HTTPException(
            status_code=400,
            detail=f"Provided token has wrong format! It should be <user_id>:<user_key>.",
        )
    return user_token.split(":")
