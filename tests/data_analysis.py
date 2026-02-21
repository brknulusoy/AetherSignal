from dotenv import load_dotenv
load_dotenv()
import boto3
import polars as pl


s3 = boto3.client("s3")
s3.download_file(
    Bucket="quantsentinel-datalake",
    Key="raw/market_data_2026-02-20.parquet",
    Filename="market_data_2026-02-20.parquet",
)

df = pl.read_parquet("market_data_2026-02-20.parquet")
print(df.head())
print(df.schema)
print(df.shape)
print(df.null_count)