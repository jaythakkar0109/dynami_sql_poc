import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    TITLE: str = "Dynamic SQL Query API - Trino Edition"
    DESCRIPTION: str = "A FastAPI service for executing dynamic SQL queries against Trino/Pinot"
    VERSION: str = "1.0.0"

    # PostgreSQL configuration
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT: int = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DATABASE: str = os.getenv('POSTGRES_DATABASE')
    POSTGRES_USER: str = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD')

    API_URL: str = os.getenv('API_URL')
    API_PORT: str = os.getenv('API_PORT')

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()