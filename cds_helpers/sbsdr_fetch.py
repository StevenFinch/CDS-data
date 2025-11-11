# cds_helpers/sbsdr_fetch.py
from __future__ import annotations
import io
import logging
from typing import Optional
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

BASE = "https://regreporting.theice.com/trade-reporting/api/v1/public-data/sbs-transaction-csv"

class FetchError(RuntimeError):
    pass

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((requests.exceptions.RequestException,))
)
def _get_csv(trade_date: str) -> Optional[pd.DataFrame]:
    headers = {
        "User-Agent": "Mozilla/5.0 (research; cds-aggregator)",
        "Accept": "text/csv, */*;q=0.1",
    }
    url = f"{BASE}?tradeDate={trade_date}"
    resp = requests.get(url, timeout=30, headers=headers)
    if resp.status_code == 404:
        # Some days truly absent
        return None
    resp.raise_for_status()
    if not resp.text.strip():
        return None
    try:
        return pd.read_csv(io.StringIO(resp.text))
    except Exception as e:
        raise FetchError(f"CSV parse failed for {trade_date}: {e}") from e

def fetch_sbsdr_day(trade_date: str) -> pd.DataFrame:
    """
    Returns DataFrame (possibly empty) of SBS transactions for the given date.
    """
    logging.info(f"[SBSR] Fetch {trade_date}")
    df = _get_csv(trade_date)
    if df is None:
        logging.info(f"[SBSR] {trade_date}: no rows")
        return pd.DataFrame()
    # Normalize columns to lower snake for easier downstream handling
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df
