"""Module with tools for access management"""
from __future__ import annotations


import logging

from fastapi import HTTPException
from db.dbmanager.dbmanager import DBManager

from .meta import LoggableMeta
from ..utils.execution import log_execution_time
from ..exceptions import AuthorizationFailed, AuthenticationFailed
from ..context import Context


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
        if DBManager().get_user_role_name(context.user.id) != "admin":
            raise AuthorizationFailed

    @classmethod
    @log_execution_time(_LOG)
    def assert_is_role_eligible(
        cls,
        product_role_name: str | None = None,
        user_role_name: str | None = None,
    ):
        """Assert that user role is eligible for the product

        Parameters
        ----------
        product_role_name : str, optional, default=None
            The role which is eligible for the given product.
            If `None`, product_role_name is claimed to be public
        user_role_name: str, optional, default=None
            The role of a user. If `None`, user_role_name is claimed
            to be public

        Raises
        -------
        AuthorizationFailed
        """
        if not cls.is_role_eligible_for_product(
            product_role_name=product_role_name, user_role_name=user_role_name
        ):
            raise AuthorizationFailed

    @classmethod
    @log_execution_time(_LOG)
    def is_role_eligible_for_product(
        cls,
        product_role_name: str | None = None,
        user_role_name: str | None = None,
    ):
        """Check if given role is eligible for the product

        Parameters
        ----------
        product_role_name : str, optional, default=None
            The role which is eligible for the given product.
            If `None`, product_role_name is claimed to be public
        user_role_name: str, optional, default=None
            The role of a user. If `None`, user_role_name is claimed
            to be public

        Returns
        -------
        is_eligible : bool
            Flag which indicate if the given `user_role_name` is eligible
             for the product with `product_role_name`
        """
        cls._LOG.debug(
            "verifying eligibility of the product role: %s against"
            " role_name %s",
            product_role_name,
            user_role_name,
        )
        if product_role_name == "public" or product_role_name is None:
            return True
        if user_role_name is None:
            # NOTE: it means, we consider the public profile
            return False
        if user_role_name == "admin":
            return True
        if user_role_name == product_role_name:
            return True
        return False

    @classmethod
    @log_execution_time(_LOG)
    def is_user_eligible_for_product(
        cls,
        context: Context,
        product_role_name: None | str = "public",
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
        user_role_name = DBManager().get_user_role_name(context.user.id)
        return cls.is_role_eligible_for_product(
            product_role_name, user_role_name
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
