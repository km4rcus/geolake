"""Module with definitions of exceptions for 'web' component"""


class AuthorizationFailed(ValueError):
    """User authorization failed"""


class AuthenticationFailed(ValueError):
    """User authentication failed"""


class MissingDatasetError(KeyError):
    """Missing dataset error"""

    def __init__(self, dataset):
        super().__init__(f"Dataset '{dataset}' is not defined.")
        self.dataset = dataset


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


class MaximumAllowedSizeExceededError(ValueError):
    """Estimated size is too big"""

    def __init__(
        self, dataset_id, product_id, estimated_size_gb, allowed_size_gb
    ):
        super().__init__(
            "Maximum allowed size for '%s.%s' is %s GB but the estimated size"
            " is %s GB",
            dataset_id,
            product_id,
            estimated_size_gb,
            allowed_size_gb,
        )


class RequestNotYetAccomplished(RuntimeError):
    """Raised if dds request was not finished yet"""


class RequestNotFound(KeyError):
    """If the given request could not be found"""
