# -*- coding: utf-8 -*-
"""Singleton module.

The module contains metaclass called <b>Singleton</b>
for thread-safe singleton-pattern implementation.
"""
from threading import Lock


class Singleton(type):
    """Thread-safe implementation of the singleton design pattern metaclass"""

    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]
