import requests


class RequestAPI:
    """
    Base class for interacting with APIs using HTTP requests.
    """

    def __init__(self):
        """
        Initialize a requests.Session object, which can be used to make
        multiple requests while efficiently reusing the underlying connection.
        """
        self.session = requests.Session()

    def _call(
        self,
        url: str,
        method: str = "GET",
        params: dict = None,
        data: dict = None,
        headers: dict = None,
    ) -> dict:
        """
        Call API with specified HTTP methods.

        Args:
            url (str): API endpoint URL
            method (str, optional): HTTP method for request. Defaults to "GET".
            params (dict, optional): Optional query parameters for request. Defaults to None.
            data (dict, optional): Optional request data for request. Defaults to None.
            headers (dict, optional): Optional headers for request. Defaults to None.

        Raises:
            APIException: Error handling during API call

        Returns:
            dict: JSON from API
        """
        try:
            response = self.session.request(
                method, url, params=params, data=data, headers=headers, timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise APIException(f"Error making API request: {e}")


class APIException(Exception):
    pass
