from datetime import date, timedelta
from src.aethersignal.config import settings
from src.aethersignal.ingestion.fetcher import fetch_market_data
from src.aethersignal.ingestion.storage import upload_to_s3

def main():
    end = date.today()
    start = end - timedelta(days=30)

    print(f"Starting ingestion for {settings.target_tickers}")

    df = fetch_market_data(
        tickers=settings.target_tickers,
        start_date=start,
        end_date=end
    )

    print(" Preview of data:")
    print(df.head())
    print(f"Shape: {df.shape}")

    file_name = f"market_data_{end}.parquet"
    s3_uri = upload_to_s3(df, file_name)

    print(f"Data has been uploaded to: {s3_uri}")

if __name__ == "__main__":
    main()