"""Module with definitions of exceptions for 'web' component"""


class AuthorizationFailed(ValueError):
    """User authorization failed"""


class AuthenticationFailed(ValueError):
    """User authentication failed"""


class MissingKeyInCatalogEntryError(KeyError):
    """Missing key in the catalog entry"""

    def __init__(self, key, dataset):
        super().__init__(
            f"There is missing '{key}' in the catalog for '{dataset}' dataset."
        )
        self.key = key
        self.dataset = dataset


class NoEligibleProductInDatasetError(ValueError):
    """No eligible products in the dataset Error"""

    def __init__(self, dataset_id: str, user_role_name) -> None:
        msg = (
            f"No eligible products for the dataset '{dataset_id}' for the user"
            f" role '{user_role_name}'"
        )
        super().__init__(msg)


class GeokubeAPIRequestFailed(RuntimeError):
    """Error while sending request to geokube-dds API"""
