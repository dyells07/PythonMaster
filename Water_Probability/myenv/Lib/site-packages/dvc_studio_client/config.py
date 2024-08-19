import re
from functools import lru_cache
from os import getenv
from typing import Any, Optional

from . import DEFAULT_STUDIO_URL, logger
from .env import (
    DVC_STUDIO_OFFLINE,
    DVC_STUDIO_REPO_URL,
    DVC_STUDIO_TOKEN,
    DVC_STUDIO_URL,
    STUDIO_REPO_URL,
    STUDIO_TOKEN,
)


def _get_remote_url() -> Optional[str]:
    from dulwich.porcelain import get_remote_repo
    from dulwich.repo import Repo

    with Repo.discover() as repo:
        try:
            _remote, url = get_remote_repo(repo)
        except IndexError:
            # IndexError happens when the head is detached
            _remote, url = get_remote_repo(repo, b"origin")
        # Dulwich returns (None, "origin") if no remote set
        if (_remote, url) == (None, "origin"):
            logger.warning("No Git remote. Can't infer Studio repo URL.")
            return None
        return url


@lru_cache(maxsize=1)
def get_studio_repo_url() -> Optional[str]:
    from dulwich.errors import NotGitRepository

    try:
        return _get_remote_url()
    except NotGitRepository:
        logger.warning(
            "Couldn't find a valid Studio Repo URL.\n"
            "You can try manually setting the environment variable `%s`.",
            STUDIO_REPO_URL,
        )
        return None


def get_studio_config(
    dvc_studio_config: Optional[dict[str, Any]] = None,
    offline: bool = False,
    studio_token: Optional[str] = None,
    studio_repo_url: Optional[str] = None,
    studio_url: Optional[str] = None,
) -> dict[str, Any]:
    """Get studio config options.

    Args:
    ----
        dvc_studio_config (Optional[dict]): Dict returned by dvc.Repo.config["studio"].
        offline (bool): Whether offline mode is enabled. Default: false.
        studio_token (Optional[str]): Studio access token obtained from the UI.
        studio_repo_url (Optional[str]): URL of the Git repository that has been
            imported into Studio UI.
        studio_url (Optional[str]): Base URL of Studio UI (if self-hosted).

    Returns:
    -------
        Dict:
            Config options for posting live metrics.
            Keys match the DVC studio config section.

    Example:
    -------
                {
                    "token": "mytoken",
                    "repo_url": "git@github.com:iterative/dvc-studio-client.git",
                    "url": "https://studio.dvc.ai",
                }
    """
    config = {}
    if not dvc_studio_config:
        dvc_studio_config = {}

    def to_bool(var):
        if var is None:
            return False
        return bool(re.search("1|y|yes|true", str(var), flags=re.I))

    offline = (
        offline
        or to_bool(getenv(DVC_STUDIO_OFFLINE))
        or to_bool(dvc_studio_config.get("offline"))
    )
    if offline:
        logger.debug("Offline mode enabled. Skipping `post_studio_live_metrics`")
        return {}

    studio_token = (
        studio_token
        or getenv(DVC_STUDIO_TOKEN)
        or getenv(STUDIO_TOKEN)
        or dvc_studio_config.get("token")
    )
    if not studio_token:
        logger.debug(
            f"{DVC_STUDIO_TOKEN} not found. Skipping `post_studio_live_metrics`",
        )
        return {}
    config["token"] = studio_token

    studio_repo_url = (
        studio_repo_url
        or getenv(DVC_STUDIO_REPO_URL)
        or getenv(STUDIO_REPO_URL)
        or dvc_studio_config.get("repo_url")
    )
    if studio_repo_url is None:
        logger.debug(
            f"{DVC_STUDIO_REPO_URL} not found. Trying to automatically find it.",
        )
        studio_repo_url = get_studio_repo_url()
    if studio_repo_url:
        config["repo_url"] = studio_repo_url
    else:
        logger.debug(
            f"{DVC_STUDIO_REPO_URL} not found. Skipping `post_studio_live_metrics`",
        )
        return {}

    studio_url = studio_url or getenv(DVC_STUDIO_URL) or dvc_studio_config.get("url")
    if studio_url:
        config["url"] = studio_url
    else:
        logger.debug(f"{DVC_STUDIO_URL} not found. Using {DEFAULT_STUDIO_URL}.")
        config["url"] = DEFAULT_STUDIO_URL

    return config
