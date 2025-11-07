from __future__ import annotations

import pathlib

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Manages application configuration loaded from .env file.

    Pydantic-settings handles validation and type coercion.
    """

    # model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    # --- Core Config ---
    DATA_DIR: pathlib.Path = pathlib.Path("/var/lib/harborpi")
    DATABASE_PATH: pathlib.Path = DATA_DIR / "captain.db"
    LOG_PATH: pathlib.Path = DATA_DIR / "logs/harborpi.log"

    # --- Web Config ---
    SERVER_PORT: int = 8080
    SERVER_HOST: str = "0.0.0.0"

    # --- GPS Config ---
    GPS_DEVICE: str = "/dev/ttyUSB0"

    # --- Logbook Logic ---
    ANCHOR_SPEED_KN: float = 0.5
    ANCHOR_MINUTES: int = 15


# Create a single, globally accessible settings instance
settings = Settings()

# Ensure data directories exist on load
settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
(settings.LOG_PATH.parent).mkdir(parents=True, exist_ok=True)
