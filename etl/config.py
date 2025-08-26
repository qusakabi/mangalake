import os
from dataclasses import dataclass

@dataclass
class Settings:
    # MinIO
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket: str = os.getenv("MINIO_BUCKET_NAME", "manga-data")

    # API
    manga_api_base: str = os.getenv("MANGA_API_BASE", "").strip()
    # Fallback to a public API to keep the stack runnable if primary DNS is unreachable
    manga_api_fallback: str = os.getenv("MANGA_API_FALLBACK", "https://api.mangadex.org/manga").strip()
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    request_retries: int = int(os.getenv("REQUEST_RETRIES", "2"))

    # Snowflake
    snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user: str = os.getenv("SNOWFLAKE_USER")
    snowflake_password: str = os.getenv("SNOWFLAKE_PASSWORD")
    snowflake_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = os.getenv("SNOWFLAKE_DATABASE")
    snowflake_schema: str = os.getenv("SNOWFLAKE_SCHEMA")

settings = Settings()
