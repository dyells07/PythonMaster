import sys
from typing import Optional, TypedDict, Union
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter

from . import logger

AVAILABLE_SCOPES = ["EXPERIMENTS", "DATASETS", "MODELS"]


class DeviceLoginResponse(TypedDict):
    device_code: str
    user_code: str
    verification_uri: str
    token_uri: str
    token_name: str
    expires_in: int


class StudioAuthError(Exception):
    pass


class InvalidScopesError(StudioAuthError):
    pass


class AuthorizationExpiredError(StudioAuthError):
    pass


def get_access_token(  # noqa: PLR0913
    *,
    hostname: str,
    token_name: Optional[str] = None,
    scopes: str = "",
    client_name: str = "client",
    open_browser: bool = True,
    post_login_message: Optional[str] = None,
    use_device_code: bool = False,
):
    """Initiate Authentication

    This method initiates the authentication process for a client application.
    It generates a user code and a verification URI that the user needs to
    access in order to authorize the application.

    Parameters
    ----------
        token_name (str): The name of the client application.
        hostname (str): The base URL of the application.
        scopes (str, optional): A comma-separated string of scopes that
            the application requires. Default is empty.
        open_browser (bool): Whether or not to open the browser to authenticate
        client_name (str, optional): Client name

    Returns
    -------
        tuple: A tuple containing the token name and the access token.
        The token name is a string representing the token's name,
        while the access token is a string representing the authorized access token.
    """
    import webbrowser

    post_login_message = post_login_message or (
        "Once you've logged in, return here "
        "and you'll be ready to start the experiments."
    )

    response = start_device_login(
        client_name=client_name,
        base_url=hostname,
        token_name=token_name,
        scopes=scopes.split(",") if scopes else [],
    )
    verification_uri = response["verification_uri"]
    user_code = response["user_code"]
    device_code = response["device_code"]
    token_uri = response["token_uri"]
    token_name = response["token_name"]
    url = f"{verification_uri}?code={user_code}"

    if use_device_code:
        open_browser = False  # backward compatibility

    if open_browser:
        print("Opening link for login at", url)  # noqa: T201
        print(f"\n{post_login_message}")  # noqa: T201
        opened = webbrowser.open(url)
        if not opened:
            print(  # noqa: T201
                "\nFailed to open a web browser. "
                "Open the above url to continue in your web browser.",
                file=sys.stderr,
            )
    else:
        print("Open this url to continue in your web browser:", url)  # noqa: T201
        print(f"\n{post_login_message}")  # noqa: T201

    access_token = check_token_authentication(uri=token_uri, device_code=device_code)
    return token_name, access_token


def start_device_login(
    *,
    client_name: str,
    base_url: Optional[str] = None,
    token_name: Optional[str] = None,
    scopes: Optional[list[str]] = None,
) -> DeviceLoginResponse:
    """This method starts the device login process for Studio.

    Parameters
    ----------
    - client_name (required): The name of the client application.

    Optional Parameters:
    - base_url: The base URL of the Studio API.
        If not provided, the default value is "https://studio.dvc.ai".
    - token_name: The name of the token. If not provided, it defaults to None.
    - scopes: A list of scopes to request. If not provided, it defaults to None.

    Returns
    -------
    - DeviceLoginResponse: A response object containing the device login information.

    Raises
    ------
    - ValueError: If any of the provided scopes are not valid.
    - RequestException: If the request fails with any 400 response or any other reason.

    """
    logger.debug(
        "Starting device login for Studio%s",
        f" ({base_url})" if base_url else "",
    )
    if scopes:
        invalid_scopes: list[str]
        if invalid_scopes := list(
            filter(lambda s: s.upper() not in AVAILABLE_SCOPES, scopes),
        ):
            raise InvalidScopesError(  # noqa: TRY003
                f"Following scopes are not valid: {', '.join(invalid_scopes)}",
            )

    body: dict[str, Union[str, list[str]]] = {"client_name": client_name}

    if token_name:
        body["token_name"] = token_name

    if scopes:
        body["scopes"] = scopes

    logger.debug(f"JSON body `{body=}`")

    response = requests.post(
        url=urljoin(base_url or "https://studio.dvc.ai", "api/device-login"),
        json=body,
        headers={
            "Content-type": "application/json",
        },
        timeout=5,
    )

    response.raise_for_status()
    d = response.json()

    logger.debug("received response: %s (status=%r)", d, response.status_code)
    return d


def check_token_authentication(*, uri: str, device_code: str) -> Optional[str]:
    """
    Checks the authentication status of a token based on a device code and
    returns access token upon successful authentication.

    Parameters
    ----------
    - uri (str): The token uri to send the request to.
    - device_code (str): The device code to check authentication for.

    Returns
    -------
    - str | None: The access token if authorized, otherwise None.

    Raises
    ------
    - requests.HTTPError: If the status code of the response is not 200.

    Example Usage:
    ```
    token = check_token_authentication(
        uri="https://example.com/api/", device_code="1234567890"
    )
    if token is not None:
        print("Access token:", token)
    else:
        print("Authentication expired.")
    ```
    """
    import time

    logger.debug("Polling to find if the user code is authorized")

    data = {"code": device_code}
    session = requests.Session()
    session.mount(uri, HTTPAdapter(max_retries=3))

    logger.debug("Checking with %s to %s", device_code, uri)

    counter = 1
    while True:
        logger.debug("Polling attempt #%s", counter)
        r = session.post(uri, json=data, timeout=5, allow_redirects=False)
        counter += 1
        if r.status_code == 400:
            d = r.json()
            detail = d.get("detail")
            if detail == "authorization_pending":
                # Wait 5 seconds before retrying.
                time.sleep(5)
                continue
            if detail == "authorization_expired":
                raise AuthorizationExpiredError(  # noqa: TRY003
                    "failed to authenticate: This 'device_code' has expired.",
                )

        r.raise_for_status()

        return r.json()["access_token"]
