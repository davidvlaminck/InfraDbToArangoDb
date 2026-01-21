from enum import Enum


class Environment(str, Enum):
    """Target environment for the MOW services."""

    PRD = 'prd'
    DEV = 'dev'
    TEI = 'tei'
    AIM = 'aim'


class AuthType(str, Enum):
    """Supported authentication types for the requesters."""

    JWT = 'JWT'
    CERT = 'cert'
    COOKIE = 'cookie'