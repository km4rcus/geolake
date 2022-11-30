"""Module with definitions of exceptions for 'web' component"""


class AuthorizationFailed(ValueError):
    """User authorization failed"""


class AuthenticationFailed(ValueError):
    """User authentication failed"""


class GeokubeAPIRequestFailed(RuntimeError):
    """Error while sending request to geokube-dds API"""
