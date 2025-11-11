# cds_helpers/clean_aggregate.py
from __future__ import annotations
import logging
from datetime import date, timedelta
from typing import Iterable, Optional, Tuple

import numpy as np
import pandas as pd

from .sbsdr_fetch import fetch_sbsdr_day, FetchFailed

log = logging.getLogger(__name__)

_MIN_SBSR_DATE = pd.Timestamp("2021-11-08").date()  # SEC SBSR go-live

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _pick_first(df: pd.DataFrame, names: Iterable[str]) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None

def _infer_spread_bps(df: pd.DataFrame) -> pd.Series:
    """
    Try a few column names that appear in SBS tapes for CDS price/spread.
    Returns a float series in basis points when feasible; NaN if absent/unusable.
    """
    cand_cols = [
        "quotedspreadsibi", "quotedspread", "cdsspread", "spreadbps", "spread", "price"
    ]
    col = _pick_first(df, cand_cols)
    if not col:
        return pd.Series(np.nan, index=df.index, dtype="float64")

    x = pd.to_numeric(df[col], errors="coerce")

    # Heuristic: valid CDS spreads are usually between 0 and 5000 bps.
    # Some tapes put decimals (like 125.5 = 125.5 bps) — both cases are fine.
    x = x.where((x >= 0) & (x <= 5000))
    return x

def _infer_notional(df: pd.DataFrame) -> pd.Series:
    cand = _pick_first(df, ["reportednotional", "notionalamount", "notional", "transactionnotional"])
    if not cand:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[cand], errors="coerce")

def _has_tenor_approx(df: pd.DataFrame, target_years: int) -> pd.Series:
    # Direct tenor columns first
    for cname in ["tenor", "producttenor", "maturitybucket", "cdstenorbucket"]:
        if cname in df.columns:
            t = df[cname].astype(str).str.upper().str.strip()
            return t.eq(f"{target_years}Y") | t.eq(f"{target_years}YR") | t.eq(f"{target_years}-YEAR")
    # Fallback: infer by termination - effective date (when provided)
    eff = _pick_first(df, ["effectivedate", "startdate"])
    end = _pick_first(df, ["terminationdate", "enddate", "maturitydate"])
    if eff and end:
        try:
            t_eff = pd.to_datetime(df[eff], errors="coerce")
            t_end = pd.to_datetime(df[end], errors="coerce")
            years = (t_end - t_eff).dt.days / 365.25
            return (years >= target_years - 0.75) & (years <= target_years + 0.75)
        except Exception:  # noqa: BLE001
            pass
    # If we can't tell, don't filter them out (return all True to avoid false negatives)
    return pd.Series(True, index=df.index)

def _in_currency(df: pd.DataFrame, cur: str) -> pd.Series:
    cur = _norm(cur)
    for cname in ["reportedcurrency", "notionalcurrency", "pricecurrency", "currency"]:
        if cname in df.columns:
            cand = df[cname].astype(str).str.lower().str.strip()
            return cand.eq(cur)
    # If unknown, don't over-filter
    return pd.Series(True, index=df.index)

def _entity_match(df: pd.DataFrame, entity: str) -> pd.Series:
    """
    Try to match reference entity across a few name-ish columns.
    """
    entity_l = _norm(entity)
    cols = [c for c in df.columns if any(k in c.lower() for k in
            ["reference", "entity", "issuer", "name", "underlier", "underlying", "security"])]
    if not cols:
        # permissive: search across all string columns concatenated
        blocks = df.apply(lambda r: " ".join([str(v) for v in r.values]), axis=1).str.lower()
        return blocks.str.contains(entity_l, na=False)
    blob = df[cols].astype(str).agg(" ".join, axis=1).str.lower()
    return blob.str.contains(entity_l, na=False)

def build_series(
    start: date,
    end: date,
    entity: str,
    tenor_years: int,
    currency: str,
    agg: str = "weighted_mean",
    min_start: Optional[date] = _MIN_SBSR_DATE,
) -> pd.DataFrame:
    """
    Build a daily series of CDS spreads (bps) for [entity, tenor, currency].
    If start < min_start (SBSR go-live), we clip and log a note.
    Ensures we *always* return rows for each calendar day with 'date' present.
    """
    # clip to SBSR coverage if requested
    clipped_start = start
    if min_start and start < min_start:
        clipped_start = min_start
        log.warning(
            "Start date %s predates SBSR public dissemination (min %s). "
            "Dates before this will be returned as NaN.",
            start, min_start,
        )

    days = pd.date_range(start, end, freq="D").date
    out_rows = []

    for d in days:
        # Pre-SBSR days: return explicit NaN row (so we never get empty frames)
        if min_start and d < clipped_start:
            out_rows.append({"date": d, "value_bps": np.nan, "count": 0, "notional_sum": 0.0})
            continue

        d_str = d.isoformat()
        log.info("[SBSR] Fetch %s", d_str)
        try:
            raw = fetch_sbsdr_day(d_str)
        except FetchFailed as e:
            log.warning("%s: fetch error: %s", d_str, e)
            out_rows.append({"date": d, "value_bps": np.nan, "count": 0, "notional_sum": 0.0})
            continue

        if raw is None or raw.empty:
            out_rows.append({"date": d, "value_bps": np.nan, "count": 0, "notional_sum": 0.0})
            continue

        # Normalize columns to lowercase
        raw.columns = [c.strip().lower() for c in raw.columns]

        # Keep only likely CDS
        # If an 'assetclass' or 'product' column is present, prefer "CD" or "CDS"
        if "assetclass" in raw.columns:
            mask_asset = raw["assetclass"].astype(str).str.upper().str.startswith("CD")
            raw = raw[mask_asset]

        # Entity / tenor / currency filters (permissive if fields missing)
        mask_ent = _entity_match(raw, entity)
        mask_ten = _has_tenor_approx(raw, tenor_years)
        mask_cur = _in_currency(raw, currency)
        sub = raw[mask_ent & mask_ten & mask_cur].copy()

        if sub.empty:
            out_rows.append({"date": d, "value_bps": np.nan, "count": 0, "notional_sum": 0.0})
            continue

        sub["spread_bps"] = _infer_spread_bps(sub)
        sub["w_notional"] = _infer_notional(sub).fillna(0.0)

        sub = sub.dropna(subset=["spread_bps"])
        if sub.empty:
            out_rows.append({"date": d, "value_bps": np.nan, "count": 0, "notional_sum": float(sub["w_notional"].sum() if "w_notional" in sub else 0.0)})
            continue

        count = len(sub)
        notional_sum = float(sub["w_notional"].sum()) if "w_notional" in sub else float(count)

        if agg == "weighted_mean" and "w_notional" in sub and sub["w_notional"].sum() > 0:
            value = float(np.average(sub["spread_bps"], weights=sub["w_notional"]))
        else:
            value = float(sub["spread_bps"].mean())

        out_rows.append(
            {"date": d, "value_bps": value, "count": int(count), "notional_sum": notional_sum}
        )

    ser = pd.DataFrame(out_rows)
    ser["date"] = pd.to_datetime(ser["date"]).dt.date
    ser = ser.sort_values("date").reset_index(drop=True)
    return ser
