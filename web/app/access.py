import logging
import requests
import secrets

from .datastore.datastore import Datastore
from .singleton import Singleton
from .util import UserCredentials


class AccessManager:

    _LOG = logging.getLogger("AccessManager")

    @classmethod
    def is_user_eligible_for_product(cls, jwt, product):
        return True

    @classmethod
    def retrieve_credentials_from_jwt(cls, header) -> UserCredentials:
        cls._LOG.debug(f"Getting credentials based on JWT...")
        r = requests.get("https://auth01.cmcc.it/auth/realms/DDS")
        keycloak_public_key = f"""-----BEGIN PUBLIC KEY-----
    {r.json()['public_key']}
    -----END PUBLIC KEY-----"""
        token = request.headers["authorization"].split(" ")[-1]
        user_id = jwt.decode(token, keycloak_public_key, audience="account")[
            "sub"
        ]
        # TODO: get user token
        user_token = "123"
        return UserCredentials(user_id=user_id, user_token=user_token)

    @classmethod
    def get_eligible_details(cls, user_cred: UserCredentials) -> dict:
        cls._LOG.debug(
            f"Getting details for eligible products of `{dataset_id}`..."
        )
        details = Datastore().dataset_info(
            dataset_id=dataset_id, use_cache=True
        )
        # TODO: get details eligible for the particular user!
        return {}
