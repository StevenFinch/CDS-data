import re
import typing as t
import pandas as pd
import numpy as np

# case-insensitive contains
def _contains_ci(series: pd.Series, token: str) -> pd.Series:
    return series.astype(str).str.contains(re.escape(token), case=False, na=False)

def find_entity_column(df: pd.DataFrame) -> str | None:
    """
    Try a set of likely columns that carry the reference entity / underlier.
    """
    LOWER = {c.lower(): c for c in df.columns}
    candidates = [c for c in df.columns if any(
        k in c.lower()
        for k in ["reference", "underlier", "underlying", "entity", "name", "obligor"]
    )]
    # Prefer more specific names first
    prio = ["referenceentityname", "referenceentity", "underliername", "underlyingname",
            "entityname", "name"]
    for p in prio:
        if p in LOWER:
            return LOWER[p]
    if candidates:
        # Choose the one with the most distinct values (likely the RE name)
        return max(candidates, key=lambda c: df[c].nunique(dropna=True))
    return None

def find_asset_class_column(df: pd.DataFrame) -> str | None:
    LOWER = {c.lower(): c for c in df.columns}
    for key in ["assetclass", "productclass", "productclassification", "assetClass"]:
        if key.lower() in LOWER:
            return LOWER[key.lower()]
    # Heuristic: a column that often says 'CDS' or 'Credit'
    for c in df.columns:
        if df[c].astype(str).str.contains("CDS|Credit", case=False, na=False).any():
            return c
    return None

def find_currency_column(df: pd.DataFrame) -> str | None:
    LOWER = {c.lower(): c for c in df.columns}
    for key in ["currency", "notionalcurrency", "pricecurrency", "sbsnotionalcurrency"]:
        if key in LOWER:
            return LOWER[key]
    # fallback: any col named like *currency*
    for c in df.columns:
        if "curr" in c.lower():
            return c
    return None

def find_tenor_column(df: pd.DataFrame) -> str | None:
    LOWER = {c.lower(): c for c in df.columns}
    for key in ["tenor", "maturitytenor", "contracttenor", "underlierTenor"]:
        if key.lower() in LOWER:
            return LOWER[key.lower()]
    for c in df.columns:
        if "tenor" in c.lower():
            return c
    return None

def find_price_column(df: pd.DataFrame) -> str | None:
    """
    We want the CDS spread if present (often 'price' with unit BPS or a 'priceNotation' field).
    """
    # Strong candidates
    for name in df.columns:
        nl = name.lower()
        if nl in ["price", "pricenotationvalue", "price_notation_value", "reportedprice"]:
            return name
    # generic fallback: any col with 'price' substring
    for name in df.columns:
        if "price" in name.lower():
            return name
    return None

def find_price_unit_column(df: pd.DataFrame) -> str | None:
    for name in df.columns:
        if "unit" in name.lower() and "price" in name.lower():
            return name
        if "price" in name.lower() and "type" in name.lower():
            return name
    # Some feeds carry 'priceNotationType' or 'priceNotation'
    for name in df.columns:
        if "pricenotation" in name.lower():
            return name
    return None

def find_notional_column(df: pd.DataFrame) -> str | None:
    for name in df.columns:
        if "notional" in name.lower() and "amount" in name.lower():
            return name
    for name in df.columns:
        if "notional" in name.lower():
            return name
    return None

def normalize_price_to_bps(price: pd.Series, unit_col: str | None, df: pd.DataFrame) -> pd.Series:
    """
    Try to interpret price as spread in bps. If unit suggests percent upfront, we keep NaN.
    """
    p = pd.to_numeric(price, errors="coerce")
    if unit_col and unit_col in df.columns:
        u = df[unit_col].astype(str).str.lower()
        # common patterns
        is_bps = u.str.contains("bp|bps|basis", na=False)
        is_pct = u.str.contains("%|percent|pct", na=False)
        # If explicitly bps, keep as-is
        p = np.where(is_bps, p, p)
        # If percent and values look like 0-100, we cannot convert reliably to bps spread for CDS upfront -> set NaN
        p = pd.Series(p)
        p[is_pct] = np.nan
        return p
    return p
