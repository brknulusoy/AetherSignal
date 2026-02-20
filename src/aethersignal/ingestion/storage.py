import boto3
import os
import polars as pl
from src.aethersignal.config import settings

def upload_to_s3(df: pl.DataFrame, file_name: str) -> str:
    """
    Saves DataFrame to Parquet locally, uploads to S3, then deletes local file.
    Returns the S3 URI.
    """
    local_path = f"data/{file_name}"
    os.makedirs("data", exist_ok=True) 

    print(f" Saving locally to {local_path}...")
    df.write_parquet(local_path)

    s3_client = boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key
    )

    s3_key = f"raw/{file_name}"
    
    print(f" Uploading to {settings.s3_bucket_name}/{s3_key}...")
    s3_client.upload_file(local_path, settings.s3_bucket_name, s3_key)

    os.remove(local_path)
    
    return f"s3://{settings.s3_bucket_name}/{s3_key}"