import json
from pathlib import Path

from API.AbstractRequester import AbstractRequester
from API.CertRequester import CertRequester
from API.APIEnums import Environment, AuthType
from API.JWTRequester import JWTRequester
from API.CookieRequester import CookieRequester


class RequesterFactory:
    """Factory for authenticated requesters.

    All callers pass:
    - auth_type (JWT/CERT/COOKIE)
    - env (PRD/TEI/DEV/AIM)
    - settings (required for JWT/CERT)

    The returned requester knows the base URL (`first_part_url`) and handles retries.
    """

    first_part_url_dict = {
        Environment.PRD: 'https://services.apps.mow.vlaanderen.be/',
        Environment.TEI: 'https://services.apps-tei.mow.vlaanderen.be/',
        Environment.DEV: 'https://services.apps-dev.mow.vlaanderen.be/',
        Environment.AIM: 'https://services-aim.apps-dev.mow.vlaanderen.be/'
    }

    @classmethod
    def create_requester(cls, auth_type: AuthType, env: Environment, settings: dict = None, cookie: str = None) -> AbstractRequester:
        first_part_url = cls.first_part_url_dict.get(env)
        if first_part_url is None:
            raise ValueError(f"Invalid environment: {env}")

        if auth_type == AuthType.COOKIE:
            if cookie is None:
                raise ValueError("argument cookie is required for COOKIE authentication")
            return CookieRequester(cookie=cookie, first_part_url=first_part_url.replace('services.', ''))

        if settings is None or 'authentication' not in settings:
            raise ValueError("invalid settings")

        specific_settings = settings['authentication'][auth_type.name][env.name.lower()]

        if auth_type == AuthType.JWT:
            return JWTRequester(
                private_key_path=specific_settings['key_path'],
                client_id=specific_settings['client_id'],
                first_part_url=first_part_url,
            )

        if auth_type == AuthType.CERT:
            return CertRequester(
                cert_path=specific_settings['cert_path'],
                key_path=specific_settings['key_path'],
                first_part_url=first_part_url,
            )

        raise ValueError(f"Invalid authentication type: {auth_type}")
