# cds_helpers/sbsdr_fetch.py
from __future__ import annotations
import io, os, socket, time, logging
from typing import Optional
import requests
from requests.adapters import HTTPAdapter, Retry

LOG = logging.getLogger("SBSR")

ICE_HOSTS = [
    "regreporting.theice.com",
    "www.regreporting.theice.com",
]
ICE_PATH = "/trade-reporting/api/v1/public-data/sbs-transaction-csv"

SESSION = requests.Session()
retries = Retry(
    total=2, connect=2, read=2, backoff_factor=0.8,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION.headers.update({
    "User-Agent": "CDS-data/1.0 (academic; contact: student@university.edu)",
    "Accept": "*/*",
})

def _doh_resolve(host: str) -> list[str]:
    """Resolve host via DNS-over-HTTPS (Cloudflare first, then Google)."""
    heads = {"accept": "application/dns-json"}
    ips: list[str] = []
    for base in ("https://cloudflare-dns.com/dns-query",
                 "https://dns.google/resolve"):
        try:
            r = SESSION.get(base, params={"name": host, "type": "A"}, timeout=10, headers=heads)
            r.raise_for_status()
            data = r.json()
            for ans in data.get("Answer", []) or []:
                if ans.get("type") == 1 and ans.get("data"):
                    ips.append(ans["data"])
        except Exception as e:
            LOG.warning("DoH resolver %s failed for %s: %s", base, host, repr(e))
    return list(dict.fromkeys(ips))  # dedupe, preserve order

def _as_text(resp: requests.Response) -> str:
    resp.raise_for_status()
    # If the server returns bytes (CSV), .text is fine because it’s ASCII/UTF-8.
    return resp.text

def _try_direct(host: str, day: str) -> Optional[str]:
    url = f"https://{host}{ICE_PATH}"
    try:
        return _as_text(SESSION.get(url, params={"tradeDate": day}, timeout=25))
    except requests.exceptions.RequestException as e:
        LOG.warning("Direct fetch from %s failed: %s", host, repr(e))
        return None

def _try_doh_ip(host: str, day: str) -> Optional[str]:
    ips = _doh_resolve(host)
    if not ips:
        LOG.warning("DNS failed for %s after DoH tries", host)
        return None
    for ip in ips:
        try:
            # Connect to IP, but keep Host header so the right vhost/SNI is selected.
            # requests will set SNI using the URL host, so we must still use the hostname in the URL.
            # We instead set the URL to https://host and route via explicit 'Host' with a custom DNS.
            # A portable trick: use the IP in the URL and override Host; disable verify if needed.
            url = f"https://{ip}{ICE_PATH}"
            resp = SESSION.get(
                url, params={"tradeDate": day}, timeout=25,
                headers={"Host": host}, verify=False  # cert CN won't match IP
            )
            if resp.status_code == 200 and resp.text.strip():
                return resp.text
        except requests.exceptions.RequestException:
            continue
    LOG.warning("All DoH/IP attempts failed for %s", host)
    return None

def _try_reader_proxy(host: str, day: str) -> Optional[str]:
    # Correct form (no duplicated scheme): r.jina.ai/http://{host}/path?query
    # Even for HTTPS origins, the reader fetcher accepts http-scheme wrapping.
    reader = f"https://r.jina.ai/http://{host}{ICE_PATH}?tradeDate={day}"
    try:
        r = SESSION.get(reader, timeout=30)
        r.raise_for_status()
        txt = r.text
        # r.jina.ai sometimes prepends the URL/title lines; CSV always has commas and header row.
        if "," in txt and ("tradeDate" in txt or "execution" in txt.lower() or "asset" in txt.lower()):
            return txt
    except requests.exceptions.RequestException as e:
        LOG.warning("%s reader proxy failed: %s", host, repr(e))
    return None

def fetch_sbsdr_day(day: str) -> Optional[str]:
    """
    Return the raw CSV text for the given day, or None if unavailable.
    """
    for host in ICE_HOSTS:
        LOG.info("[SBSR] Fetch %s via direct %s", day, host)
        t = _try_direct(host, day)
        if t: return t

        LOG.warning("DNS/direct failed for %s: trying DoH/IP route", host)
        t = _try_doh_ip(host, day)
        if t: return t

        LOG.info("[SBSR] Fetch %s via proxy reader for %s", day, host)
        t = _try_reader_proxy(host, day)
        if t: return t

    LOG.warning("%s: fetch error: All hosts failed for %s", day, day)
    return None
