"""Settings."""

# pylint: disable=too-few-public-methods

import pathlib
from typing import Optional

import pydantic


class Settings(pydantic.BaseSettings):
    """Application settings class."""

    debug = False
    testing = False
    log_dir: Optional[pathlib.Path] = None

    class Config:
        """Config metadata for the settings."""

        case_sensitive = False
        env_prefix = "WL_"


settings = Settings()
