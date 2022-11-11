"""Module containing utils for geokube-dds API accessing"""
from __future__ import annotations
import os
import logging
import requests

from .utils import UserCredentials, log_execution_time
from .meta import LoggableMeta
from .exceptions import GeokubeAPIRequestFailed


class GeokubeAPIRequester(metaclass=LoggableMeta):
    """The class handling requests to geokube dds API"""

    _LOG = logging.getLogger("GeokubeAPIRequester")
    _API_URL: str = None
    _IS_INIT: bool = False

    @classmethod
    def init(cls):
        """Initialize class with API URL"""
        cls._API_URL = os.environ.get("API_URL", "https://ddshub.cmcc.it/api")
        cls._LOG.info(
            "'API_URL' environment variable collected: %s", cls._API_URL
        )
        cls._IS_INIT = True

    @staticmethod
    def _get_http_header_from_user_credentials(
        user_credentials: UserCredentials | None = None,
    ):
        if user_credentials is not None and user_credentials.id is not None:
            return {
                "User-Token": (
                    f"{user_credentials.id}:{user_credentials.user_token}"
                )
            }
        return {}

    @classmethod
    def _prepare_headers(cls, user_credentials):
        headers = {
            "Content-Type": "application/json",
        }
        headers.update(
            GeokubeAPIRequester._get_http_header_from_user_credentials(
                user_credentials
            )
        )
        # "User-Token": "d9152e98-9de8-4064-b281-f61f8cecffe9:arZFgTatrOJpJ3egHEjRUyTUDt763SX6uAI4m2CVT4I",
        return headers

    @classmethod
    @log_execution_time(_LOG)
    def post(
        cls,
        url: str,
        data: str,
        user_credentials: UserCredentials | None = None,
    ):
        """
        Send POST request to geokube-dds API

        Parameters
        ----------
        url : str
            Path to which the query should be send. It is created as
            f"{GeokubeAPIRequester._API_URL}{url}"
        data : str
            JSON payload of the request
        user_credentials : UserCredentials
            Credentials of the current user

        Returns
        -------
        response : str
            Response from geokube-dds API

        Raises
        -------
        GeokubeAPIRequestFailed
            If request failed due to any reason
        """
        assert cls._IS_INIT, "GeokubeAPIRequester was not initialized!"
        target_url = f"{cls._API_URL}{url}"
        headers = cls._prepare_headers(user_credentials)
        cls._LOG.debug("sending POST request to %s", target_url)
        cls._LOG.debug("payload of the POST request: %s", data)
        response = requests.post(
            target_url,
            data=data,
            headers=headers,
            timeout=11,
        )
        if response.status_code != 200:
            raise GeokubeAPIRequestFailed(
                response.json().get(
                    "detail", "Request to geokube-dds API failed!"
                )
            )
        if "application/json" in response.headers.get("Content-Type", ""):
            return response.json()
        return response.text()

    @classmethod
    @log_execution_time(_LOG)
    def get(
        cls,
        url: str,
        user_credentials: UserCredentials | None = None,
    ):
        """
        Send GET request to geokube-dds API

        Parameters
        ----------
        url : str
            Path to which the query should be send. It is created as
            f"{GeokubeAPIRequester._API_URL}{url}"
        user_credentials : UserCredentials
            Credentials of the current user

        Returns
        -------
        response : str
            Response from geokube-dds API

        Raises
        -------
        GeokubeAPIRequestFailed
            If request failed due to any reason
        """
        assert cls._IS_INIT, "GeokubeAPIRequester was not initialized!"
        target_url = f"{cls._API_URL}{url}"
        headers = cls._prepare_headers(user_credentials)
        cls._LOG.debug("sending GET request to %s", target_url)
        response = requests.get(
            target_url,
            headers=headers,
            timeout=11,
        )
        if response.status_code != 200:
            cls._LOG.info(
                "request to geokube-dds API failed due to: %s", response.text
            )
            raise GeokubeAPIRequestFailed(
                response.json().get(
                    "detail", "Request to geokube-dds API failed!"
                )
            )
        return response.json()
