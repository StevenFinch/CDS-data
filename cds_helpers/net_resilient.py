# cds_helpers/net_resilient.py
import io, time, json, requests

def _doh_ipv4(host: str, timeout=8) -> list[str]:
    """
    Resolve A records via Google's DNS over HTTPS to bypass runner DNS hiccups.
    Returns a list of IPv4 strings or [] if none.
    """
    url = f"https://dns.google/resolve?name={host}&type=A"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    js = r.json()
    answers = js.get("Answer", []) or []
    return [a.get("data") for a in answers if a.get("type") == 1 and a.get("data")]

def _curl_with_resolve(url: str, host: str, ip: str, timeout=60) -> str:
    """
    Fetch URL by pinning host->ip with pycurl --resolve, preserving TLS SNI/Host.
    """
    import pycurl
    buf = io.BytesIO()
    c = pycurl.Curl()
    c.setopt(pycurl.URL, url)
    c.setopt(pycurl.TIMEOUT, timeout)
    c.setopt(pycurl.SSL_VERIFYPEER, 1)
    c.setopt(pycurl.SSL_VERIFYHOST, 2)
    c.setopt(pycurl.HTTPHEADER, [f"Host: {host}", "User-Agent: cds-pipeline/1.0"])
    c.setopt(pycurl.RESOLVE, [f"{host}:443:{ip}"])  # bypass system DNS
    c.setopt(pycurl.WRITEDATA, buf)
    c.perform()
    code = c.getinfo(pycurl.RESPONSE_CODE)
    c.close()
    if 200 <= code < 300:
        return buf.getvalue().decode("utf-8", errors="replace")
    raise RuntimeError(f"pycurl fetch got HTTP {code} via {ip}")

def get_url_resilient(url: str, host: str, timeout=60, tries=5, backoff=2.0) -> str:
    """
    Try normal requests first; on DNS/connect errors, fallback to DoH + pycurl --resolve.
    Exponential backoff across attempts.
    """
    last_err = None
    # 1) direct (uses runner DNS)
    for attempt in range(tries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(backoff ** attempt)

    # 2) DoH -> pycurl --resolve (bypass runner DNS)
    ips = []
    for attempt in range(tries):
        try:
            ips = _doh_ipv4(host, timeout=timeout//4)
            if ips:
                break
        except Exception as e:
            last_err = e
        time.sleep(backoff ** attempt)

    if not ips:
        raise RuntimeError(f"DoH failed for {host}") from last_err

    for ip in ips:
        for attempt in range(tries):
            try:
                return _curl_with_resolve(url, host, ip, timeout=timeout)
            except Exception as e:
                last_err = e
                time.sleep(backoff ** attempt)

    raise RuntimeError(f"All fetch attempts failed for {url}") from last_err
