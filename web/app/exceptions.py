"""Module with definitions of exceptions for 'web' component"""


class AuthenticationFailed(ValueError):
    """User authentication failed"""


class GeokubeAPIRequestFailed(RuntimeError):
    """Error while sending request to geokube-dds API"""


class UserAlreadyExistError(KeyError):
    """Given user already exists"""
