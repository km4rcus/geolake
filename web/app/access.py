import logging
import requests
import secrets

from .datastore.datastore import Datastore
from .singleton import Singleton
from .util import UserCredentials


class AccessManager:

    _LOG = logging.getLogger("AccessManager")

    @classmethod
    def is_user_eligible_for_product(cls, user_role_name, product_role_name):
        cls._LOG.debug(
            "Verifying eligibility of the user_id:"
            f" {user_credentials.id} against role_name: {product_role_name}"
        )
        if product_role_name == "public":
            return True
        if user_credentials.is_public:
            return False
        if user_role_name == "admin":
            return True
        elif user_role_name == product_role_name:
            return True
        else:
            return False

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
    def get_eligible_products_names(cls, user_cred: UserCredentials) -> dict:
        cls._LOG.debug("Getting all eligible products...")
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        # TODO:
        pass


    @classmethod
    def get_eligible_details(cls, user_cred: UserCredentials) -> dict:
        cls._LOG.debug(
            f"Getting details for eligible products of `{dataset_id}`..."
        )
        user_role_name = DBManager().get_user_role_name(user_credentials.id)
        details = Datastore().dataset_info(
            dataset_id=dataset_id, use_cache=True
        )
        eligible_products = {}
        if (products := details.get("products")) and isinstance(
            products, dict
        ):
            for prod_name, prod in products.items():
                assert (
                    "metadata" in prod
                ), f"Metadata are not defined for the product `{prod_name}`"
                metadata = prod["metadata"]
                if cls.is_user_eligible_for_product(
                    user_role_name=user_role_name,
                    product_role_name=metadata.get("role", "public"),
                ):
                    eligible_products[prod_name] = prod
        details["products"] = eligible_products
        return details
