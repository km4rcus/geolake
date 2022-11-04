"""Module containing utils for geokube-dds API accessing"""
from __future__ import annotations
import os
import logging
from typing import Any

import aiohttp

from .util import UserCredentials, log_execution_time
from .meta import LoggableMeta


class Requester(metaclass=LoggableMeta):
    _LOG = logging.getLogger("Requester")
    _API_URL: str = None
    _IS_INIT: bool = False

    @classmethod
    def init(cls):
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
    @log_execution_time
    async def post(
        cls,
        url: str,
        data: Any,
        params: dict | None = None,
        user_credentials: UserCredentials | None = None,
    ):
        assert cls._IS_INIT, "Requester was not initialized!"
        if params is None:
            params = {}
        async with aiohttp.ClientSession(cls._API_URL) as session:
            async with session.post(
                url,
                data=data,
                params=params,
                headers=Requester._get_http_header_from_user_credentials(
                    user_credentials
                ),
            ) as resp:
                cls._LOG.debug(
                    "'%s' responded with the status: '%d'", url, resp.status
                )
                return resp.json()
