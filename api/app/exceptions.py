"""Module with DDS exceptions definitions"""
from fastapi import HTTPException


class BaseDDSException(BaseException):
    """Base class for DDS.api exceptions"""

    msg: str

    def wrap_around_http_exception(self) -> HTTPException:
        """Wrap an exception around `fastapi.HTTPExcetion`"""
        return HTTPException(
            status_code=404,
            detail=self.msg,
        )


class EmptyUserTokenError(BaseDDSException):
    """Raised if `User-Token` is empty"""

    def wrap_around_http_exception(self) -> HTTPException:
        raise HTTPException(
            status_code=400, details="User-Token cannot be empty!"
        )


class ImproperUserTokenError(BaseDDSException):
    """Raised if `User-Token` format is wrong"""

    def wrap_around_http_exception(self) -> HTTPException:
        raise HTTPException(
            status_code=400,
            details=(
                "The format of the User-Token is wrong. It should be be in the"
                " format <user_id (UUID v4)>:<api_key (string)>!"
            ),
        )


class NoEligibleProductInDatasetError(BaseDDSException):
    """No eligible products in the dataset Error"""

    def __init__(self, dataset_id: str, user_roles_names: list[str]) -> None:
        self.msg = (
            f"No eligible products for the dataset '{dataset_id}' for the user"
            f" with roles '{user_roles_names}'"
        )
        super().__init__(self.msg)


class MissingKeyInCatalogEntryError(BaseDDSException):
    """Missing key in the catalog entry"""

    def __init__(self, key, dataset):
        self.msg = (
            f"There is missing '{key}' in the catalog for '{dataset}' dataset."
        )
        super().__init__(self.msg)


class MaximumAllowedSizeExceededError(BaseDDSException):
    """Estimated size is too big"""

    def __init__(
        self, dataset_id, product_id, estimated_size_gb, allowed_size_gb
    ):
        self.msg = (
            f"Maximum allowed size for '{dataset_id}.{product_id}' is"
            f" {allowed_size_gb} GB but the estimated size is"
            f" {estimated_size_gb} GB"
        )
        super().__init__(self.msg)


class RequestNotYetAccomplished(BaseDDSException):
    """Raised if dds request was not finished yet"""

    def __init__(self, request_id):
        self.msg = (
            f"Request with id: {request_id} does not exist or it is not"
            " finished yet!"
        )
        super().__init__(self.msg)


class RequestNotFound(BaseDDSException):
    """If the given request could not be found"""

    def __init__(self, request_id: int) -> None:
        self.msg = f"Request with ID '{request_id}' was not found"
        super().__init__(self.msg)


class RequestStatusNotDone(BaseDDSException):
    """Raised when the submitted request failed"""

    def __init__(self, request_id, request_status) -> None:
        self.msg = (
            f"Request with id: `{request_id}` does not have download. URI. Its"
            f" status is: `{request_status}`!"
        )
        super().__init__(self.msg)


class AuthorizationFailed(BaseDDSException):
    """Raised when the user is not authorized for the given resource"""

    def __init__(self, user_id: str | None):
        if user_id is None:
            self.msg = "Anonymous user is not authorized for the resource!"
        else:
            self.msg = f"User 's{user_id}' is not authorized for the resource!"
        super().__init__(self.msg)


class AuthenticationFailed(BaseDDSException):
    """Raised when the key of the provided user differs from the one s
    tored in the DB"""

    def __init__(self, user_id: str):
        self.msg = "Authentication of the user '{user_id}' failed!"
        super().__init__(self.msg)


class MissingDatasetError(BaseDDSException):
    """Raied if the queried dataset is not present in the catalog"""

    def __init__(self, dataset_id: str):
        self.msg = f"Dataset '{dataset_id}' does not exist in the catalog!"
        super().__init__(self.msg)


class MissingProductError(BaseDDSException):
    """Raised if the requested product is not defined for the dataset"""

    def __init__(self, dataset_id: str, product_id: str):
        self.msg = (
            f"Product '{dataset_id}.{product_id}' does not exist in the"
            " catalog!"
        )
        super().__init__(self.msg)
