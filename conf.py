from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Conf(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    ENV: str
    
    DEBUG: bool

    ROOT_DIR: Path = Path(__file__).parent
    LOG_DIR: Path = ROOT_DIR / 'logs'

    KASPAD_HOST_1: str

    DB_URI: str

conf = Conf()