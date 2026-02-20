from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    aws_region: str = 'eu-north-1'
    s3_bucket_name: str

    target_tickers: list[str] = ["AAPL", "MSFT", "GOOGL"]

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
