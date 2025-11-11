# cds_helpers/investing_fetch.py

import datetime as dt
import pandas as pd
import requests
from bs4 import BeautifulSoup

INVESTING_URL = (
    "https://www.investing.com/rates-bonds/united-states-cds-5-years-usd-historical-data"
)

def fetch_investing_history():
    """
    Scrape the full historical table shown on Investing.com for US 5Y CDS.
    Returns DataFrame with columns ['date','cds_bps'] where cds_bps is float.
    NOTE:
    - Site sometimes requires headers to avoid 403.
    - We assume US-style mm/dd/yyyy or similar and parse to datetime.date.
    """
    headers = {
        "User-Agent":
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/112.0 Safari/537.36"
    }
    resp = requests.get(INVESTING_URL, headers=headers, timeout=30)
    if resp.status_code != 200:
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Heuristic: find table rows in the main historical data table.
    rows = soup.find_all("tr")
    data = []
    for r in rows:
        cols = r.find_all("td")
        if len(cols) < 2:
            continue
        date_txt = cols[0].get_text(strip=True)
        val_txt = cols[1].get_text(strip=True)
        # date like "Nov 07, 2025" etc.
        # try multiple parsers
        parsed_date = None
        for fmt in ("%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                parsed_date = dt.datetime.strptime(date_txt, fmt).date()
                break
            except Exception:
                pass
        if parsed_date is None:
            continue

        def to_float(x):
            x = x.replace(",", "").replace("%", "")
            try:
                return float(x)
            except:
                return None
        val = to_float(val_txt)
        if val is None:
            continue

        data.append({"date": parsed_date, "cds_bps": val})

    if not data:
        return pd.DataFrame()

    out = pd.DataFrame(data).drop_duplicates(subset=["date"]).sort_values("date")
    return out.reset_index(drop=True)
