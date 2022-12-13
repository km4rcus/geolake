"""Module with definitions of exceptions for 'web' component"""
from fastapi import HTTPException


class DDSException:
    """Base class for DDS web exceptions"""

    def wrap_around_http_error(self, **values):
        """Create an instance of fastapi.HTTPException"""
        raise NotImplementedError


class AuthorizationFailed(ValueError, DDSException):
    """User authorization failed"""

    def wrap_around_http_error(self, **values):
        return HTTPException(
            status_code=401,
            detail="User is not authorized!",
        )


class AuthenticationFailed(ValueError, DDSException):
    """User authentication failed"""

    def wrap_around_http_error(self):
        return HTTPException(status_code=400, detail="Authentication failed!")


class MissingDatasetError(KeyError, DDSException):
    """Missing dataset error"""

    def __init__(self, dataset):
        super().__init__(f"Dataset '{dataset}' is not defined.")
        self.dataset = dataset

    def wrap_around_http_error(self, **values):
        return HTTPException(
            status_code=400,
            detail="Dataset '{dataset_id}' does not exist!".format(**values),
        )


class MissingKeyInCatalogEntryError(KeyError, DDSException):
    """Missing key in the catalog entry"""

    def __init__(self, key, dataset):
        super().__init__(
            f"There is missing '{key}' in the catalog for '{dataset}' dataset."
        )
        self.key = key
        self.dataset = dataset

    def wrap_around_http_error(self, **values):
        return HTTPException(
            status_code=400,
            detail=(
                "Product '{product_id}' for the dataset '{dataset_id}' does"
                " not exist!".format(**values)
            ),
        )


class NoEligibleProductInDatasetError(ValueError, DDSException):
    """No eligible products in the dataset Error"""

    def __init__(self, dataset_id: str, user_role_name) -> None:
        msg = (
            f"No eligible products for the dataset '{dataset_id}' for the user"
            f" role '{user_role_name}'"
        )
        super().__init__(msg)


class MaximumAllowedSizeExceededError(ValueError, DDSException):
    """Estimated size is too big"""

    def __init__(
        self, dataset_id, product_id, estimated_size_gb, allowed_size_gb
    ):
        super().__init__(
            f"Maximum allowed size for '{dataset_id}.{product_id}' is"
            f" {estimated_size_gb} GB but the estimated size is"
            f" {allowed_size_gb} GB"
        )

    def wrap_around_http_error(self, **values):
        return HTTPException(
            status_code=400, detail="{details}".format(**values)
        )


class RequestNotYetAccomplished(RuntimeError, DDSException):
    """Raised if dds request was not finished yet"""

    def wrap_around_http_error(self, **values):
        return HTTPException(
            status_code=404,
            detail=(
                "Request with id: {request_id} does not exist or it is"
                " not finished yet!".format(**values)
            ),
        )


class RequestNotFound(KeyError, DDSException):
    """If the given request could not be found"""

    def wrap_around_http_error(self, **values):
        return HTTPException(
            status_code=400,
            detail="Request with ID '{request_id}' was not found!".format(
                **values
            ),
        )
