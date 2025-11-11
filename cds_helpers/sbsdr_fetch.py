import datetime as dt
from cds_helpers.net_resilient import get_url_resilient

ICE_HOST = "regreporting.theice.com"
ICE_URL_TMPL = "https://regreporting.theice.com/trade-reporting/api/v1/public-data/sbs-transaction-csv?tradeDate={d}"

def fetch_sbsdr_day(date_str: str) -> str:
    """
    Returns CSV text for a given YYYY-MM-DD from ICE SBSDR.
    Raises on HTTP/connection failure after resilient attempts.
    """
    url = ICE_URL_TMPL.format(d=date_str)
    return get_url_resilient(url, host=ICE_HOST, timeout=60, tries=4, backoff=1.6)

def probe_day(day: dt.date) -> bool:
    """
    Lightweight probe: returns True if the endpoint returns any data (status 200 and non-empty).
    """
    try:
        txt = fetch_sbsdr_day(day.strftime("%Y-%m-%d"))
        return bool(txt.strip())
    except Exception:
        return False
