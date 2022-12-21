"""Modules realizing logic for user-related endpoints"""
from typing import Optional

from pydantic import BaseModel
from db.dbmanager.dbmanager import DBManager

from ..auth import Context, assert_not_anonymous
from ..api_logging import get_dds_logger
from ..metrics import log_execution_time

log = get_dds_logger(__name__)


class UserDTO(BaseModel):
    """DTO class containing information about a user to store in the DB"""

    contact_name: str
    user_id: Optional[str] = None
    api_key: Optional[str] = None
    roles: Optional[list[str]] = None


@log_execution_time(log)
@assert_not_anonymous
def add_user(context: Context, user: UserDTO):
    """Add a user to the database

    Parameters
    ----------
    context : Context
        Context of the current http request
    user: UserDTO
        User to be added

    Returns
    -------
    user_id : UUID
        ID of the newly created user in the database
    """
    # TODO: some admin priviliges check
    return DBManager().add_user(
        contact_name=user.contact_name,
        user_id=user.user_id,
        api_key=user.api_key,
        roles_names=user.roles,
    )
