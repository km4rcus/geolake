"""Module with tools for access management"""
import logging
from typing import Optional

from pydantic import BaseModel
from db.dbmanager.dbmanager import DBManager

from .meta import LoggableMeta
from ..decorators import log_execution_time
from ..exceptions import AuthorizationFailed
from ..context import Context


class User(BaseModel):
    contact_name: str
    user_id: Optional[str] = None
    api_key: Optional[str] = None
    roles: Optional[list[str]] = None


class AccessManager(metaclass=LoggableMeta):
    """Manager that handles access to the geokube-dds"""

    _LOG = logging.getLogger("geokube.AccessManager")

    @classmethod
    def assert_is_admin(cls, context: Context) -> bool:
        """Assert that user has an 'admin' role

        Parameters
        ----------
        context : Context
            Context of the current http request

        Raises
        -------
        AuthorizationFailed
        """
        if "admin" not in DBManager().get_user_roles_names(context.user.id):
            raise AuthorizationFailed

    @classmethod
    @log_execution_time(_LOG)
    def assert_is_role_eligible(
        cls,
        product_role_name: Optional[str] = None,
        user_roles_names: Optional[list[str]] = None,
    ):
        """Assert that user role is eligible for the product

        Parameters
        ----------
        product_role_name : str, optional, default=None
            The role which is eligible for the given product.
            If `None`, product_role_name is claimed to be public
        user_roles_names: list of str, optional, default=None
            A list of user roles names. If `None`, user_roles_names is claimed
            to be public

        Raises
        -------
        AuthorizationFailed
        """
        if not cls.is_role_eligible_for_product(
            product_role_name=product_role_name,
            user_roles_names=user_roles_names,
        ):
            raise AuthorizationFailed

    @classmethod
    @log_execution_time(_LOG)
    def is_role_eligible_for_product(
        cls,
        product_role_name: Optional[str] = None,
        user_roles_names: Optional[list[str]] = None,
    ):
        """Check if given role is eligible for the product

        Parameters
        ----------
        product_role_name : str, optional, default=None
            The role which is eligible for the given product.
            If `None`, product_role_name is claimed to be public
        user_roles_names: list of str, optional, default=None
            A list of user roles names. If `None`, user_roles_names is claimed
            to be public

        Returns
        -------
        is_eligible : bool
            Flag which indicate if any role within the given `user_roles_names`
            is eligible for the product with `product_role_name`
        """
        cls._LOG.debug(
            "verifying eligibility of the product role '%s' against"
            " roles '%s'",
            product_role_name,
            user_roles_names,
        )
        if product_role_name == "public" or product_role_name is None:
            return True
        if user_roles_names is None:
            # NOTE: it means, we consider the public profile
            return False
        if "admin" in user_roles_names:
            return True
        if product_role_name in user_roles_names:
            return True
        return False

    @classmethod
    @log_execution_time(_LOG)
    def is_user_eligible_for_product(
        cls,
        context: Context,
        product_role_name: Optional[None] = "public",
    ) -> bool:
        """Check if user is eligible for the given product's role.
        If no product role name is defined, it's treated as the 'public'
        profile.

        Parameters
        ----------
        context : Context
            Context of the current http request
        product_role_name : str, optional, default="public"
            The name of the product's role

        Returns
        -------
        is_eligible : bool
            `True` if user is eligible, `False` otherwise
        """
        cls._LOG.debug(
            "verifying eligibility of the user_id '%s' against role_name:"
            " '%s'",
            context.user.id,
            product_role_name,
        )
        if product_role_name is None or product_role_name == "public":
            return True
        if context.user.is_public:
            return False
        user_roles_names = DBManager().get_user_roles_names(context.user.id)
        return cls.is_role_eligible_for_product(
            product_role_name, user_roles_names
        )

    @classmethod
    @log_execution_time(_LOG)
    def assert_user_eligible_for_request(
        cls, context: Context, request_id: int
    ) -> bool:
        """Check if user is eligible to see request's details

        Parameters
        ----------
        context : Context
            Context of the current http request
        request_id : int, optional, default="public"
            ID of the request to check

        Raises
        ------
        AuthorizationFailed
            If user is not authorized for the given dds request
        """
        cls._LOG.debug(
            "verifying eligibility of the user_id: '%s' against request_id:"
            " '%s'",
            context.user.id,
            request_id,
        )
        request_details = DBManager().get_request_details(
            request_id=request_id
        )
        if (request_details is not None) and (
            str(request_details.user_id) == str(context.user.id)
        ):
            return
        raise AuthorizationFailed

    @classmethod
    @log_execution_time(_LOG)
    def add_user(cls, context: Context, user: User):
        """Add a user to the database

        Parameters
        ----------
        context : Context
            Context of the current http request
        user: User
            User to be added

        Returns
        -------
        user_id : UUID
            ID of the newly created user in the database
        """
        return DBManager().add_user(
            contact_name=user.contact_name,
            user_id=user.user_id,
            api_key=user.api_key,
            roles_names=user.roles,
        )
