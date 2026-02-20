from datetime import date
import polars as pl
import yfinance as yf
import pandas as pd

def fetch_market_data(tickers: list[str], start_date: date, end_date: date) -> pl.DataFrame:
    """
    Fetches historical data for multiple tickers and returns a combined Polars DataFrame.
    
    Schema:
    - ticker: str
    - date: date
    - open, high, low, close: float
    - volume: int
    """
    
    print(f"Downloading data for: {tickers}...")

    data_frames = []

    for ticker in tickers:   
        finance_data = yf.download(ticker, start=start_date, end=end_date, progress=False)

        if finance_data.empty:
            print(f"No data found for {ticker}")
            continue
        
        if isinstance(finance_data.columns, pd.MultiIndex):
            finance_data.columns = finance_data.columns.get_level_values(0)

        finance_data = finance_data.reset_index()

        ldf = pl.from_pandas(finance_data)
        ldf = ldf.rename({col: col.lower() for col in ldf.columns})

        ldf = ldf.with_columns(pl.lit(ticker).alias("ticker"))
        data_frames.append(ldf)

    if not data_frames:
        raise ValueError("No data was downloaded for any ticker.")

    return pl.concat(data_frames)