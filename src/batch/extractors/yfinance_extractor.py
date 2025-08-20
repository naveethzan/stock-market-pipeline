import os
import io
import json
import time
import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq

from batch.utils.s3_utils import put_bytes, write_success_marker, copy_object, delete_object

logger = logging.getLogger(__name__)


def _retry(delays: List[float]):
    """Simple retry decorator factory with given delays in seconds."""
    def decorator(fn):
        def wrapper(*args, **kwargs):
            last_err = None
            for i, d in enumerate(delays, start=1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_err = e
                    logger.warning(f"Attempt {i} failed: {e}. Retrying in {d}s...")
                    time.sleep(d)
            # Final attempt
            return fn(*args, **kwargs)
        return wrapper
    return decorator


@_retry([0.5, 1.0, 2.0])
def _fetch_symbol_day(symbol: str, process_date: str) -> pd.DataFrame:
    start = datetime.strptime(process_date, "%Y-%m-%d")
    end = start + timedelta(days=1)
    df = yf.download(
        tickers=symbol,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        progress=False,
        auto_adjust=False,
        threads=False,
    )
    if isinstance(df, pd.Series):
        df = df.to_frame().T
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","symbol","open","high","low","close","volume","adj_close","ingestion_timestamp","source"])  # empty
    df = df.reset_index()
    # Expected columns: ['Date','Open','High','Low','Close','Adj Close','Volume']
    rename_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df[v] = df[k]
        elif v not in df.columns:
            df[v] = pd.NA
    df["symbol"] = symbol
    df["ingestion_timestamp"] = datetime.utcnow().isoformat() + "Z"
    df["source"] = "yahoo_finance"
    # Cast types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    numeric_cols = ["open","high","low","close","adj_close","volume"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["symbol","date","open","high","low","close","volume","adj_close","ingestion_timestamp","source"]]


def fetch_eod_batch(symbols: List[str], process_date: str) -> pd.DataFrame:
    """Fetch EOD data for list of symbols for a single process date."""
    frames = []
    for sym in symbols:
        try:
            f = _fetch_symbol_day(sym, process_date)
            if not f.empty:
                frames.append(f)
            else:
                logger.info(f"No data for {sym} on {process_date}")
        except Exception as e:
            logger.error(f"Failed to fetch {sym}: {e}")
    if not frames:
        return pd.DataFrame(columns=["symbol","date","open","high","low","close","volume","adj_close","ingestion_timestamp","source"])
    return pd.concat(frames, ignore_index=True)


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, raw_base_uri: str, process_date: str) -> Dict[str, Any]:
    """Write a single Parquet file using temp/commit pattern under date-partitioned prefix."""
    dt = datetime.strptime(process_date, "%Y-%m-%d")
    year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
    prefix = f"{raw_base_uri}/batch/year={year}/month={month}/day={day}/"
    key = f"{prefix}data-{process_date}.parquet"
    tmp_prefix = f"{prefix}_tmp/"
    tmp_key = f"{tmp_prefix}data-{process_date}-{uuid.uuid4().hex}.parquet"

    # Convert to Arrow Table
    table = pa.Table.from_pandas(df, preserve_index=False)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    # 1) Write to temp
    put_bytes(bucket=bucket, key=tmp_key, data=sink.getvalue(), content_type="application/octet-stream")
    # 2) Commit: copy temp -> final
    copy_object(bucket=bucket, source_key=tmp_key, dest_key=key)
    # 3) Cleanup temp
    try:
        delete_object(bucket=bucket, key=tmp_key)
    except Exception as _:
        logger.warning(f"Failed to delete temp object s3://{bucket}/{tmp_key}")

    # Write _SUCCESS marker
    write_success_marker(bucket=bucket, prefix=prefix)

    return {"prefix": prefix, "keys": [key], "row_count": int(df.shape[0])}


def extract_to_s3_parquet(process_date: str, bucket: str, raw_base_uri: str, symbols: List[str]) -> Dict[str, Any]:
    """High-level extractor: fetch EOD data and write to a partitioned Parquet file in S3 (Parquet only)."""
    if not bucket:
        raise ValueError("AWS S3 bucket is required")
    if not raw_base_uri:
        raw_base_uri = "raw-data"

    logger.info(f"Starting raw extract for {process_date}; symbols={len(symbols)}")
    df = fetch_eod_batch(symbols, process_date)
    if df.empty:
        logger.warning(f"No data fetched for {process_date}. Still writing empty Parquet for idempotency.")
        # Ensure minimal schema
        df = pd.DataFrame(columns=["symbol","date","open","high","low","close","volume","adj_close","ingestion_timestamp","source"])

    result = write_parquet_to_s3(df, bucket=bucket, raw_base_uri=raw_base_uri, process_date=process_date)
    logger.info(f"Wrote raw Parquet: s3://{bucket}/{result['keys'][0]} rows={result['row_count']}")
    return result
