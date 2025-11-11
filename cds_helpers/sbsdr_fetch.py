# cds_helpers/sbsdr_fetch.py
from __future__ import annotations

import io
import os
import time
import socket
import logging
from typing import Optional, Tuple, List

import pandas as pd
import requests
from tenacity import (
    retry, stop_after_attempt, wait_exponential_jitter,
    retry_if_exception_type, before_sleep_log
)

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# Prefer the bare host (the www subdomain is flaky on some runners)
_DEFAULT_HOSTS = [
    "regreporting.theice.com",
    # keep www as a last resort (some ISPs only resolve this)
    "www.regreporting.theice.com",
]

def _hosts_from_env() -> List[str]:
    env = os.getenv("ICE_HOSTS", "").strip()
    if not env:
        return list(_DEFAULT_HOSTS)
    return [h.strip() for h in env.split(",") if h.strip()]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; CI runner) Python-requests",
    "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

def _dns_ok(host: str, attempts: int = 4, sleep_s: float = 1.5) -> bool:
    """Best-effort DNS preflight so we fail fast to the next host."""
    for i in range(attempts):
        try:
            socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
            return True
        except socket.gaierror as e:
            if i == attempts - 1:
                LOG.warning("DNS failed for %s after %d tries: %s", host, attempts, e)
                return False
            time.sleep(sleep_s)
    return False

class HttpError(RuntimeError):
    pass

@retry(
    retry=retry_if_exception_type((requests.RequestException, HttpError)),
    wait=wait_exponential_jitter(initial=2, max=60, jitter=1.0),
    stop=stop_after_attempt(6),
    before_sleep=before_sleep_log(LOG, logging.WARNING),
    reraise=True,
)
def _fetch_once(url: str, timeout: Tuple[float, float] = (10.0, 90.0)) -> bytes:
    resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
    # ICE returns 200 with CSV; on maintenance it can return HTML
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if resp.status_code != 200:
        raise HttpError(f"HTTP {resp.status_code} for {url}")
    return resp.content

def _build_url(host: str, day: str) -> str:
    # ICE SBSR daily CSV endpoint
    return f"https://{host}/trade-reporting/api/v1/public-data/sbs-transaction-csv?tradeDate={day}"

def fetch_sbsdr_day(day: str, raw_dir: Optional[str] = "data/raw") -> Optional[pd.DataFrame]:
    """
    Download ICE SBSR daily public CSV for `day` (YYYY-MM-DD).
    Returns a DataFrame or None if not available. Also saves a raw .csv.gz snapshot.
    """
    hosts = _hosts_from_env()
    last_err: Optional[Exception] = None
    for host in hosts:
        url = _build_url(host, day)
        if not _dns_ok(host):
            last_err = RuntimeError(f"DNS unavailable: {host}")
            continue
        LOG.info("[SBSR] Fetch %s via %s", day, host)
        try:
            blob = _fetch_once(url)
            # Heuristic: if it's very small or looks like HTML, keep snapshot but skip parse
            if raw_dir:
                import gzip, pathlib
                pathlib.Path(raw_dir).mkdir(parents=True, exist_ok=True)
                out_path = pathlib.Path(raw_dir) / f"sbs_{day}.csv.gz"
                with gzip.open(out_path, "wb") as gz:
                    gz.write(blob)
            # Try parse as CSV; if it fails, we still keep the snapshot
            try:
                df = pd.read_csv(io.BytesIO(blob))
            except Exception as pe:
                LOG.warning("CSV parse failed for %s (%s); keeping raw snapshot", day, type(pe).__name__)
                return None
            # Empty days happen (holidays / outages)
            if df is None or df.empty:
                LOG.info("[SBSR] %s returned 0 rows", day)
                return None
            return df
        except Exception as e:
            last_err = e
            LOG.warning("%s: fetch error: %s", day, e)
            # Try next host
            continue
    LOG.warning("All hosts failed for %s: %s", day, last_err)
    return None
