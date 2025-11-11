# cds_helpers/clean_aggregate.py
from __future__ import annotations

import logging
from typing import Iterable, Optional, Literal, Dict, Any, List
import pandas as pd
from .sbsdr_fetch import fetch_sbsdr_day

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# column alias maps (lowercased)
REF_ENTITY_KEYS = [
    "referenceentity", "reference_entity", "referenceentityname",
    "entity", "name", "underlier", "underlyingreference", "underlying_name",
    "sbse_reference_entity_name", "reference_index_underlier"
]
CCY_KEYS = ["currency", "settlementcurrency", "notionalcurrency", "ccy"]
TENOR_KEYS = ["tenor", "maturitytenor", "quotedtenor"]
SPREAD_KEYS = ["parspread", "par_spread", "quotedspread", "spread", "price"]

def _lc_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    # preserve original, but we will search case-insensitively
    return df

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k in cols:
            return cols[k]
    # fallback: try startswith for messy header variants
    for c in df.columns:
        lc = c.lower()
        if any(lc.startswith(k) for k in candidates):
            return c
    return None

def _to_bps(v: pd.Series) -> pd.Series:
    # If typical magnitudes look like 50–300 => bps already
    # If typical magnitudes < 3 => probably decimal (0.0123 => 123 bps)
    s = pd.to_numeric(v, errors="coerce")
    if s.dropna().empty:
        return s
    med = s.dropna().abs().median()
    if pd.isna(med):
        return s
    if med < 3:  # decimal
        return s * 1_00 * 100  # 1.00% == 100 bps; decimal(0.01) -> 100 bps
    return s

def _match_entity(text: str, entity_tokens: List[str]) -> bool:
    t = (text or "").lower()
    return all(tok in t for tok in entity_tokens)

def build_series(
    start_date: str,
    end_date: str,
    entity: str,
    tenor_years: int = 5,
    currency: Optional[str] = "USD",
    aggregation: Literal["weighted_mean","median","mean"] = "weighted_mean",
) -> pd.DataFrame:
    """
    Iterate days, fetch SBSR CSV, filter to CDS rows matching entity/tenor/currency,
    aggregate to daily par-spread (bps). Returns DataFrame[date, par_spread_bps, n_trades].
    """
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    rows: List[Dict[str, Any]] = []
    entity_tokens = [w for w in entity.lower().replace(".", "").split() if w]

    for day in dates:
        ds = day.strftime("%Y-%m-%d")
        df = fetch_sbsdr_day(ds, raw_dir="data/raw")
        if df is None or df.empty:
            continue

        df = _lc_columns(df)

        # Basic filters: asset class & product
        asset_col = _find_col(df, ["assetclass"])
        prod_col  = _find_col(df, ["product", "producttype"])
        if asset_col:
            df = df[df[asset_col].astype(str).str.lower().str.contains("credit", na=False)]
        if prod_col:
            df = df[df[prod_col].astype(str).str.lower().str.contains("cds|credit default", na=False)]

        if df.empty:
            continue

        # Reference entity filter
        ref_col = _find_col(df, REF_ENTITY_KEYS)
        if ref_col:
            df = df[df[ref_col].astype(str).apply(lambda x: _match_entity(x, entity_tokens))]
        # Currency filter
        if currency:
            ccy_col = _find_col(df, CCY_KEYS)
            if ccy_col:
                df = df[df[ccy_col].astype(str).str.upper() == currency.upper()]

        # Tenor filter (keep lightly, many feeds call it '5Y', '5y', 'P5Y')
        ten_col = _find_col(df, TENOR_KEYS)
        if ten_col:
            df = df[df[ten_col].astype(str).str.contains("5", case=False, na=False)]

        if df.empty:
            continue

        # Spread field
        sp_col = _find_col(df, SPREAD_KEYS)
        if not sp_col:
            # No quoted spread; skip this day politely
            LOG.info("No spread column on %s; kept raw, skipping aggregation", ds)
            continue

        # Try weights (by notional if present)
        w_col = _find_col(df, ["notionalamount", "notional", "quantity"])
        weights = None
        if w_col:
            weights = pd.to_numeric(df[w_col], errors="coerce")

        spreads = _to_bps(df[sp_col])

        # Build daily aggregate
        day_spread = None
        if aggregation == "weighted_mean" and w_col and weights.notna().any():
            try:
                day_spread = (spreads * weights).sum() / weights.sum()
            except Exception:
                day_spread = spreads.mean()
        elif aggregation == "median":
            day_spread = spreads.median()
        else:
            day_spread = spreads.mean()

        if pd.isna(day_spread):
            continue

        rows.append({"date": ds, "par_spread_bps": float(day_spread), "n_trades": int(len(df))})

    # Return tidy frame; if empty, return schema to avoid KeyError
    if not rows:
        LOG.warning("No data collected between %s and %s for entity=%s", start_date, end_date, entity)
        return pd.DataFrame(columns=["date", "par_spread_bps", "n_trades"])

    out = pd.DataFrame(rows)
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return out
