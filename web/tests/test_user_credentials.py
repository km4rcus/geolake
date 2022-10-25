import pytest
import uuid


from pydantic import ValidationError

from app.util import UserCredentials


class TestUserCredentials:
    def test_use_wrong_user_id_format(self):
        with pytest.raises(ValidationError):
            UserCredentials(user_id=10, user_token="aaa")

    def test_uuidv4_user_id(self):
        id_ = uuid.uuid4()
        uc = UserCredentials(user_id=id_, user_token="aaa")
        assert uc.id == id_

    def test_str_uuidv4_user_id(self):
        id_ = str(uuid.uuid4())
        uc = UserCredentials(user_id=id_, user_token="aaa")
        assert type(id_) != type(uc.id)
        assert str(uc.id) == id_

    def test_ensure_key_is_not_printed(self):
        uc = UserCredentials(user_token="aaa")
        repr_val = repr(uc)
        assert uc.key == "aaa"
        assert "aaa" not in repr_val
