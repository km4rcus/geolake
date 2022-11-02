"""Module with exceptions definitions"""


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
