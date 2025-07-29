import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    TITLE: str = "Dynamic SQL Query API - Trino Edition"
    DESCRIPTION: str = "A FastAPI service for executing dynamic SQL queries against Trino/Pinot"
    VERSION: str = "1.0.0"

    # Trino configuration
    TRINO_HOST: str = os.getenv('TRINO_HOST')
    TRINO_PORT: int = int(os.getenv('TRINO_PORT'))
    TRINO_HTTP_SCHEME: str = os.getenv('TRINO_HTTP_SCHEME', 'https')
    TRINO_USERNAME: str = os.getenv('TRINO_USERNAME')
    TRINO_PASSWORD: str = os.getenv('TRINO_PASSWORD')
    TRINO_SCHEMA: str = os.getenv('TRINO_SCHEMA', 'default')
    TRINO_VERIFY: bool = os.getenv('TRINO_VERIFY', 'False').lower() in ('true', '1', 't')

    # PostgreSQL configuration
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT: int = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DATABASE: str = os.getenv('POSTGRES_DATABASE')
    POSTGRES_USER: str = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD')

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()