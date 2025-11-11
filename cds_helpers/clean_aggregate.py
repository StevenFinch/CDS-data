from __future__ import annotations
import datetime as dt
import typing as t
import pandas as pd
import numpy as np
from .sbsdr_fetch import fetch_sbsdr_day
from .parsing import (
    find_entity_column, find_asset_class_column, find_currency_column,
    find_tenor_column, find_price_column, find_price_unit_column,
    find_notional_column, normalize_price_to_bps, _contains_ci
)

def _date_range(start: str, end: str) -> list[str]:
    s = dt.date.fromisoformat(start)
    e = dt.date.fromisoformat(end)
    out = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out

def _is_public_start(d: dt.date) -> bool:
    # Public SEC SBSDR tape really starts 2022-02-14.
    return d >= dt.date(2022, 2, 14)

def _match_entity(df: pd.DataFrame, name_like: str) -> pd.Series:
    """
    Fuzzy match 'United States of America' across any entity/underlier-like column.
    """
    col = find_entity_column(df)
    if not col:
        return pd.Series([False] * len(df))
    tokens = [t for t in name_like.split() if t]
    mask = pd.Series([True] * len(df))
    for tok in tokens:
        mask &= _contains_ci(df[col], tok)
    # also allow shortcuts like 'USA' or 'UNITED STATES'
    short = _contains_ci(df[col], "UNITED STATES") | _contains_ci(df[col], "USA")
    return mask | short

def build_series(entity_name: str, start: str, end: str,
                 currency: str = "USD",
                 tenor_prefer: str = "5",
                 agg: str = "weighted_mean") -> pd.DataFrame:
    """
    Returns a daily time series with columns:
      date, n_trades, notional_sum, spread_bps_{agg}
    """
    dates = _date_range(start, end)
    rows = []

    for ds in dates:
        d = dt.date.fromisoformat(ds)
        if not _is_public_start(d):
            # skip pre-public-tape to avoid noise
            continue

        raw = fetch_sbsdr_day(ds)
        if raw.empty:
            continue

        # Identify key columns
        asset_col = find_asset_class_column(raw)
        curr_col  = find_currency_column(raw)
        tenor_col = find_tenor_column(raw)
        price_col = find_price_column(raw)
        unit_col  = find_price_unit_column(raw)
        notl_col  = find_notional_column(raw)

        df = raw.copy()

        # Filter to CDS
        if asset_col:
            df = df[df[asset_col].astype(str).str.contains("CDS|Credit", case=False, na=False)]

        # Filter to currency
        if curr_col:
            df = df[df[curr_col].astype(str).str.upper().eq(currency.upper())]

        # Filter to entity (sovereign US)
        m_ent = _match_entity(df, entity_name)
        df = df[m_ent]

        if df.empty:
            continue

        # Prefer 5y tenor if we can find a tenor-ish column
        if tenor_col and df[tenor_col].notna().any():
            is5 = df[tenor_col].astype(str).str.contains("5", case=False, na=False)
            if is5.any():
                df = df[is5]

        if df.empty:
            continue

        # Price in bps
        if not price_col:
            continue
        price_bps = normalize_price_to_bps(df[price_col], unit_col, df)
        notl = pd.to_numeric(df[notl_col], errors="coerce") if notl_col else pd.Series([np.nan]*len(df))
        w = notl.where(notl > 0, np.nan)

        # Aggregate
        val = None
        if agg == "weighted_mean" and w.notna().any():
            val = np.nansum(price_bps * w) / np.nansum(w)
        else:
            val = np.nanmean(price_bps)

        rows.append({
            "date": ds,
            "n_trades": int(len(df)),
            "notional_sum": float(np.nansum(w) if w is not None else np.nan),
            "spread_bps": float(val) if val == val else np.nan,
        })

    return pd.DataFrame(rows).sort_values("date")
