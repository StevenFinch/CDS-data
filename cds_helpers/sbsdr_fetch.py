# cds_helpers/sbsdr_fetch.py
from __future__ import annotations
import io, logging, time, socket, ipaddress
from typing import Optional, Tuple, List
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Optional DNS override
try:
    import dns.resolver  # dnspython
except Exception:
    dns = None  # we’ll guard usage below

LOG = logging.getLogger("SBSR")

ICE_HOSTS = [
    "regreporting.theice.com",
    "www.regreporting.theice.com",
]
PATH = "/trade-reporting/api/v1/public-data/sbs-transaction-csv"

SESSION = requests.Session()
ADAPTER = requests.adapters.HTTPAdapter(
    max_retries=requests.packages.urllib3.util.retry.Retry(
        total=2, read=2, connect=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504]
    )
)
SESSION.mount("https://", ADAPTER)
SESSION.mount("http://", ADAPTER)

class AllHostsFailed(Exception):
    pass

def _resolve_via_public_dns(host: str) -> Optional[str]:
    """Resolve A/AAAA using public resolvers to bypass runner DNS. Returns first IP as str or None."""
    if dns is None:
        return None
    resolvers = []
    for nameserver in ( "8.8.8.8", "1.1.1.1" ):
        r = dns.resolver.Resolver(configure=False)
        r.nameservers = [nameserver]
        r.lifetime = 3.0
        resolvers.append(r)
    # prefer A then AAAA
    for r in resolvers:
        try:
            ans = r.resolve(host, "A")
            if ans:
                return ans[0].to_text()
        except Exception:
            pass
    for r in resolvers:
        try:
            ans = r.resolve(host, "AAAA")
            if ans:
                return ans[0].to_text()
        except Exception:
            pass
    return None

def _ip_url(ip: str, path: str, params: str) -> str:
    # We’ll talk to the IP directly, keep https, and set Host header separately.
    return f"https://{ip}{path}?{params}"

def _direct_url(host: str, path: str, params: str) -> str:
    return f"https://{host}{path}?{params}"

def _proxy_url(host: str, path: str, params: str) -> str:
    # Last-resort proxy fetcher (textual passthrough). Works for CSV content.
    # We encode the target URL after /http:// — the reader will fetch remotely.
    target = f"https://{host}{path}?{params}"
    return f"https://r.jina.ai/http://{target}"

@retry(
    reraise=True,
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=0.8, min=0.8, max=4.0),
    retry=retry_if_exception_type((requests.RequestException,))
)
def _try_get(url: str, *, host_header: Optional[str] = None, verify: bool = True, timeout: int = 25) -> requests.Response:
    headers = {"User-Agent": "CDS-fetch/1.0 (+github actions)", "Accept": "*/*"}
    if host_header:
        headers["Host"] = host_header
    resp = SESSION.get(url, timeout=timeout, headers=headers, verify=verify)
    resp.raise_for_status()
    return resp

def _safe_text(resp: requests.Response) -> str:
    # Some days return empty body but 200; normalize to ""
    return resp.text or ""

def fetch_sbsdr_day(day_iso: str) -> str:
    """
    Returns CSV text (possibly empty string) for given YYYY-MM-DD tradedate,
    trying multiple hosts + DNS bypass + proxy.
    """
    params = f"tradeDate={day_iso}"
    last_err: Optional[Exception] = None

    for host in ICE_HOSTS:
        # 1) normal
        try:
            url = _direct_url(host, PATH, params)
            LOG.info("[SBSR] Fetch %s via direct %s", day_iso, host)
            return _safe_text(_try_get(url))
        except Exception as e:
            LOG.warning("%s: direct fetch from %s failed: %s", day_iso, host, repr(e))
            last_err = e

        # 2) DNS bypass to IP with Host header (TLS verify off to survive SNI/Cert mismatch)
        ip = _resolve_via_public_dns(host)
        if ip:
            try:
                if ":" in ip:  # IPv6 => wrap in []
                    ip_fmt = f"[{ip}]"
                else:
                    ip_fmt = ip
                url = _ip_url(ip_fmt, PATH, params)
                LOG.info("[SBSR] Fetch %s via IP %s (Host:%s)", day_iso, ip, host)
                return _safe_text(_try_get(url, host_header=host, verify=False))
            except Exception as e:
                LOG.warning("%s: IP fetch %s (Host:%s) failed: %s", day_iso, ip, host, repr(e))
                last_err = e
        else:
            LOG.warning("DNS failed for %s: could not resolve via 8.8.8.8/1.1.1.1", host)

        # 3) proxy
        try:
            url = _proxy_url(host, PATH, params)
            LOG.info("[SBSR] Fetch %s via proxy reader for %s", day_iso, host)
            return _safe_text(_try_get(url))
        except Exception as e:
            LOG.warning("%s: proxy fetch for %s failed: %s", day_iso, host, repr(e))
            last_err = e

    raise AllHostsFailed(f"All hosts failed for {day_iso}: {last_err!r}")
