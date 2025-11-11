import io
import time
import typing as t
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

CSV_PATH = "/trade-reporting/api/v1/public-data/sbs-transaction-csv?tradeDate={date}"
HOSTS = [
    "https://regreporting.theice.com",  # primary
    # Fallback host kept for resilience; if it 404s, we just try next.
    "https://icetradevault.com",
]

class FetchError(Exception):
    pass

@retry(
    retry=retry_if_exception_type((requests.RequestException, FetchError)),
    wait=wait_exponential(multiplier=1, min=1, max=20),
    stop=stop_after_attempt(5),
    reraise=True
)
def _get_csv_text(url: str, timeout: int = 30) -> str:
    resp = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0 (ICE-SBSDR-CDS-fetch/1.0)"},
        timeout=timeout,
    )
    if resp.status_code == 404:
        raise FetchError(f"404 at {url}")
    resp.raise_for_status()
    text = resp.text
    # crude sanity: must look like CSV header (contain comma and some known tokens)
    if "," not in text or len(text) < 50:
        raise FetchError("Response doesn't look like CSV")
    return text

def fetch_sbsdr_day(date_str: str) -> pd.DataFrame:
    """
    Returns a pandas DataFrame of *all* SBSDR prints on a given date.
    If nothing available (holiday/early dates), returns empty DataFrame.
    """
    last_err = None
    for host in HOSTS:
        url = f"{host}{CSV_PATH.format(date=date_str)}"
        try:
            csv_text = _get_csv_text(url)
            df = pd.read_csv(io.StringIO(csv_text))
            # Normalize columns for downstream logic
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception as e:
            last_err = e
            time.sleep(0.5)
            continue
    # If all hosts fail, return empty but informative (caller will handle)
    return pd.DataFrame()

