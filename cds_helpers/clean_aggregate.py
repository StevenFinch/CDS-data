# cds_helpers/clean_aggregate.py
from __future__ import annotations
import io, logging, re
from dataclasses import dataclass
from typing import Literal, Optional, Tuple
import pandas as pd
from .sbsdr_fetch import fetch_sbsdr_day

LOG = logging.getLogger("SBSR")

Agg = Literal["weighted_mean", "median", "mean"]

ENTITY_PAT = re.compile(r"(united\s*states\s*of\s*america|^usa$|u\.?s\.?a\.?)", re.I)
TENOR_PAT = re.compile(r"\b5\s*[- ]?\s*y", re.I)  # match 5Y, 5-Y, 5 Years

COLMAP = {
    "date": ["tradeDate", "asOfDate", "eventDate", "executionDate", "executionTimestamp"],
    "reference": [
        "referenceEntity", "referenceentity", "reference_name", "underlierName",
        "underlyingName", "referenceEntityName", "entityName"
    ],
    "currency": ["notionalCurrency", "priceCurrency", "currency", "dealCurrency"],
    "tenor": ["tenor", "maturityTenor", "expirationTenor", "maturityBucket"],
    "product": ["product", "instrument", "assetSubtype"],
    "asset": ["assetClass", "assetclass", "asset_type"],
    "notional": ["notional", "notionalAmount", "priceNotional", "quantity", "reportedNotional"],
    "spread": ["spread", "price", "fixedRate", "coupon", "strike", "executedPrice"],
}

def _coerce_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    # unify case-insensitively by adding a lower-case view
    lower = {c.lower(): c for c in df.columns}
    def pick(name_group):
        for c in name_group:
            cl = c.lower()
            if cl in lower:
                yield lower[cl]
            for k in lower:
                if cl == k:
                    yield lower[k]
    return df

def _first_col(df: pd.DataFrame, keys: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for k in keys:
        if k.lower() in cols:
            return cols[k.lower()]
    # fuzzy: exact match ignoring non-letters
    norm = {re.sub(r"[^a-z]", "", c.lower()): c for c in df.columns}
    for k in keys:
        nk = re.sub(r"[^a-z]", "", k.lower())
        if nk in norm:
            return norm[nk]
    return None

def _read_csv(text: str) -> Optional[pd.DataFrame]:
    if not text or "," not in text:
        return None
    try:
        df = pd.read_csv(io.StringIO(text))
        if df.empty or len(df.columns) < 3:
            return None
        return df
    except Exception:
        # Some days contain stray BOM/lines; try python engine / skipbadlines
        try:
            df = pd.read_csv(io.StringIO(text), engine="python", on_bad_lines="skip")
            return df if not df.empty else None
        except Exception:
            return None

def _filter_usa_usd_5y(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Standardize column picks
    c_date = _first_col(df, COLMAP["date"]) or "tradeDate"
    c_ref  = _first_col(df, COLMAP["reference"])
    c_ccy  = _first_col(df, COLMAP["currency"])
    c_ten  = _first_col(df, COLMAP["tenor"])
    c_prod = _first_col(df, COLMAP["product"])
    c_ast  = _first_col(df, COLMAP["asset"])
    c_not  = _first_col(df, COLMAP["notional"])
    c_spr  = _first_col(df, COLMAP["spread"])

    # Lowercase string cols safely
    for c in [c_ref, c_ccy, c_ten, c_prod, c_ast]:
        if c and c in df.columns:
            df[c] = df[c].astype(str)

    # Asset must be Credit/CDS-ish
    if c_ast in df and df[c_ast].notna().any():
        df = df[df[c_ast].str.contains("credit|cr", case=False, regex=True)]
    if c_prod in df and df[c_prod].notna().any():
        df = df[df[c_prod].str.contains("cds", case=False, regex=True)]

    # Reference entity contains USA
    if c_ref in df:
        df = df[df[c_ref].str.contains(ENTITY_PAT, na=False)]
    else:
        # If we cannot see the reference name, we can't assert USA—return empty
        return df.iloc[0:0].copy()

    # Currency USD if we can see it
    if c_ccy in df:
        df = df[df[c_ccy].str.upper().str.contains(r"\bUSD\b", na=False)]

    # Tenor ~5Y if any tenor column exists; otherwise keep all (we'll downselect by maturity bucket if present)
    if c_ten in df and df[c_ten].notna().any():
        df = df[df[c_ten].str.contains(TENOR_PAT, na=False)]

    # Guard: must have a date column
    if c_date not in df:
        return df.iloc[0:0].copy()

    # Coerce numeric for notional/spread candidates
    if c_not in df:
        df[c_not] = pd.to_numeric(df[c_not], errors="coerce")
    if c_spr in df:
        df[c_spr] = pd.to_numeric(df[c_spr], errors="coerce")

    # Drop rows where both notional and spread are missing
    if c_not in df and c_spr in df:
        df = df[(df[c_not].notna()) | (df[c_spr].notna())]

    # Standardize output columns
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[c_date], errors="coerce").dt.date
    out["entity"] = "United States of America"
    out["currency"] = "USD"
    out["tenor_years"] = 5

    if c_not in df:
        out["notional"] = df[c_not].fillna(0.0)
    else:
        out["notional"] = 0.0

    if c_spr in df:
        out["spread_bps"] = df[c_spr]
    else:
        out["spread_bps"] = pd.NA

    return out.dropna(subset=["date"])

def build_series(start_date: str, end_date: str, agg: Agg = "weighted_mean") -> pd.DataFrame:
    """
    Iterate days, fetch CSV, filter for USA 5Y USD single-name CDS, aggregate by day.
    Returns DataFrame with columns:
      date, entity, tenor_years, currency, trades, notional, price_bps_<agg>
    """
    from datetime import date, timedelta
    s = pd.to_datetime(start_date).date()
    e = pd.to_datetime(end_date).date()
    if e < s:
        raise ValueError("end_date before start_date")

    rows = []
    d = s
    while d <= e:
        ds = d.isoformat()
        LOG.info("[SBSR] Fetch %s", ds)
        txt = fetch_sbsdr_day(ds)
        if txt:
            dfraw = _read_csv(txt)
        else:
            dfraw = None

        if dfraw is not None and not dfraw.empty:
            ser = _filter_usa_usd_5y(dfraw)
            if not ser.empty:
                # daily aggregation
                trades = len(ser)
                notional = ser["notional"].fillna(0.0).sum()
                if agg == "weighted_mean" and ser["spread_bps"].notna().any() and notional > 0:
                    price = (ser["spread_bps"].fillna(0.0) * ser["notional"].fillna(0.0)).sum() / max(notional, 1e-12)
                elif agg == "median" and ser["spread_bps"].notna().any():
                    price = ser["spread_bps"].median()
                elif agg == "mean" and ser["spread_bps"].notna().any():
                    price = ser["spread_bps"].mean()
                else:
                    price = None

                rows.append({
                    "date": ser["date"].iloc[0],
                    "entity": "United States of America",
                    "tenor_years": 5,
                    "currency": "USD",
                    "trades": trades,
                    "notional": float(notional),
                    f"price_bps_{agg}": (None if price is None else float(price)),
                })
        d += timedelta(days=1)

    if not rows:
        return pd.DataFrame(columns=["date","entity","tenor_years","currency","trades","notional",f"price_bps_{agg}"])

    out = pd.DataFrame(rows).sort_values("date")
    out["date"] = pd.to_datetime(out["date"]).dt.date
    # De-duplicate per day (keep last non-null)
    out = out.drop_duplicates(subset=["date"], keep="last")
    return out
