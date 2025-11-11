# cds_helpers/sbsdr_fetch.py
from __future__ import annotations
import io
import logging
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

log = logging.getLogger(__name__)

# ICE Trade Vault U.S. SBSR public dissemination (CSV per tradeDate)
_BASES = [
    "https://regreporting.theice.com",
    "https://www.regreporting.theice.com",  # fallback host
]
_PATH = "/trade-reporting/api/v1/public-data/sbs-transaction-csv"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; cds-bot/1.0)",
    "Accept": "text/csv, */*;q=0.1",
}

class FetchFailed(Exception):
    pass

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=1, max=10),
    retry=retry_if_exception_type(requests.RequestException),
)
def _attempt_fetch(url: str) -> pd.DataFrame:
    resp = requests.get(url, headers=_HEADERS, timeout=45)
    resp.raise_for_status()
    # ICE returns CSV; sometimes a BOM is present
    return pd.read_csv(io.BytesIO(resp.content))

def fetch_sbsdr_day(date_str: str) -> pd.DataFrame:
    """
    Fetch one SBSR CSV for a YYYY-MM-DD date. Returns a DataFrame (possibly empty).
    Raises FetchFailed only if *all* base URLs fail.
    """
    last_err = None
    for base in _BASES:
        url = f"{base}{_PATH}?tradeDate={date_str}"
        try:
            df = _attempt_fetch(url)
            log.info("[SBSR] %s -> %d rows", date_str, len(df))
            return df
        except Exception as e:  # noqa: BLE001
            last_err = e
            log.warning("[SBSR] %s fetch failed from %s: %s", date_str, base, e)
            continue
    raise FetchFailed(f"All hosts failed for {date_str}: {last_err}")
