"""
Settings for the application
"""

import os
from dotenv import load_dotenv, set_key
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_DIR / '.env'

load_dotenv(dotenv_path=ENV_PATH)

class Settings:
    """
    Handles settings for the application
    """
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'nfl')
    DB_USER = os.getenv('DB_USER', 'masori')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'changeme')
    DEFAULT_PASSWORD = os.getenv('DEFAULT_PASSWORD', 'changeme')

    # superuser info
    PG_USER = os.getenv('POSTGRES_USER', None)
    PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', None)

    def update_db_password(self, new_pw: str) -> None:
        set_key(ENV_PATH, "DB_PASSWORD", new_pw)
        self.DB_PASSWORD = new_pw


settings = Settings()
