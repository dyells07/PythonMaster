"""DVC Studio Client."""

import logging
from os import getenv

from .env import DVC_STUDIO_CLIENT_LOGLEVEL, DVCLIVE_LOGLEVEL

logger = logging.getLogger("dvc_studio_client")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(
    getenv(DVC_STUDIO_CLIENT_LOGLEVEL, getenv(DVCLIVE_LOGLEVEL, "WARNING")).upper(),
)

DEFAULT_STUDIO_URL = "https://studio.dvc.ai"
