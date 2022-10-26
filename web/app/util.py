from typing import Optional
from pydantic import BaseModel, UUID4


class UserCredentials(BaseModel):
    user_id: Optional[UUID4] = None
    user_token: Optional[str] = None

    @property
    def is_public(self) -> bool:
        return self.user_id is None

    @property
    def id(self) -> str:
        return self.user_id

    @property
    def key(self) -> str:
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
