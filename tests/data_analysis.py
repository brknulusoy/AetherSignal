from dotenv import load_dotenv
load_dotenv()
import boto3
import polars as pl
import os

bucket = os.getenv("AWS_S3_BUCKET")
key = os.getenv("AWS_S3_KEY")
filename = os.getenv("AWS_S3_FILENAME")


s3 = boto3.client("s3")
s3.download_file(
    Bucket=bucket,
    Key=key,
    Filename=filename,
)

df = pl.read_parquet(filename)
print(df.head())
print(df.schema)
print(df.shape)
print(df.null_count())