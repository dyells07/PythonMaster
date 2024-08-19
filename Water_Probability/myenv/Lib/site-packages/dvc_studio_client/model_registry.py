from typing import Optional
from urllib.parse import urljoin

import requests
from requests.exceptions import RequestException

from . import logger
from .config import get_studio_config

MODEL_REGISTRY_API_PATH = "api/model-registry"
GET_DOWNLOAD_URIS_PATH = f"{MODEL_REGISTRY_API_PATH}/get-download-uris"


def get_download_uris(
    repo: str,
    name: str,
    version: Optional[str] = None,
    stage: Optional[str] = None,
    **kwargs,
) -> dict[str, str]:
    """Return download URIs for the specified model.

    Args:
    ----
        repo: Git repo URL.
        name: Model name.
        version: Model version.
        stage: Model stage.

    Additional keyword arguments will be passed to get_studio_config().

    Raises:
    ------
        ValueError: Invalid arguments were passed or the API call failed.
    """
    config = get_studio_config(**kwargs)
    if not config:
        raise ValueError("No studio config")  # noqa: TRY003
    params = {"repo": repo, "name": name}
    if version and stage:
        raise ValueError("Version and stage are mutually exclusive")  # noqa: TRY003
    if version:
        params["version"] = version
    if stage:
        params["stage"] = stage

    try:
        url = urljoin(config["url"], GET_DOWNLOAD_URIS_PATH)
        response = requests.get(
            url,
            params=params,
            headers={"Authorization": f"token {config['token']}"},
            timeout=(30, 5),
        )
    except RequestException as e:
        raise ValueError("Failed to reach studio API") from e  # noqa: TRY003

    if response.status_code != 200:
        message = response.content.decode()
        logger.debug(
            "get_download_uris: %d '%s'",
            response.status_code,
            message,
        )
        raise ValueError(f"Failed to get model download URIs from studio: {message}")  # noqa: TRY003
    return response.json()
