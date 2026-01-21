import abc

from requests import Session, Response


class AbstractRequester(Session, metaclass=abc.ABCMeta):
    """Base HTTP requester with simple retry-on-non-2xx behavior.

    Subclasses typically add authentication (JWT/cert/cookie).

    Notes:
    - `first_part_url` is prefixed to all request URLs.
    - Retries are only based on status code (no exponential backoff).
    """

    def __init__(self, first_part_url: str = '', retries: int = 3):
        super().__init__()
        self.first_part_url = first_part_url

        if retries < 1:
            raise ValueError("retries must be at least 1")
        self.retries = retries

    def _request_with_retries(self, method_name: str, url: str = '', **kwargs) -> Response:
        """Execute a request method on Session with retries.

        We treat any 2xx response as success.
        """
        response: Response | None = None
        for _ in range(self.retries):
            method = getattr(super(), method_name)
            response = method(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        raise RuntimeError(f"{method_name.upper()} request failed after {self.retries} retries. Last response: {response}")

    @abc.abstractmethod
    def get(self, url: str = '', **kwargs) -> Response:
        return self._request_with_retries('get', url=url, **kwargs)

    @abc.abstractmethod
    def post(self, url: str = '', **kwargs) -> Response:
        return self._request_with_retries('post', url=url, **kwargs)

    @abc.abstractmethod
    def put(self, url: str = '', **kwargs) -> Response:
        return self._request_with_retries('put', url=url, **kwargs)

    @abc.abstractmethod
    def patch(self, url: str = '', **kwargs) -> Response:
        return self._request_with_retries('patch', url=url, **kwargs)

    @abc.abstractmethod
    def delete(self, url: str = '', **kwargs) -> Response:
        return self._request_with_retries('delete', url=url, **kwargs)
