# cds_helpers/clean_aggregate.py
from __future__ import annotations
import logging
from typing import Iterable, Optional, Callable
import pandas as pd
import numpy as np

from .sbsdr_fetch import fetch_sbsdr_day

def _contains(s: str, needle: str) -> bool:
    return needle.lower() in (s or "").lower()

def _pick_price(df: pd.DataFrame) -> Optional[pd.Series]:
    """
    Try several likely price columns in ICE SBSR. We prefer 'spread' if present.
    """
    candidates = [
        "spread", "spread_rate", "price", "execution_price", "price1", "price2",
        "price_notation", "price_notation_value"
    ]
    for c in candidates:
        if c in df.columns and np.issubdtype(df[c].dtype, np.number):
            return df[c].astype(float)
        if c in df.columns:
            # try to coerce
            try:
                return pd.to_numeric(df[c], errors="coerce")
            except Exception:
                pass
    # Sometimes price hides in a JSON-like column; leave None if not found
    return None

def _pick_notional(df: pd.DataFrame) -> Optional[pd.Series]:
    candidates = ["notional_amount", "notional", "effective_notional", "quantity", "qty"]
    for c in candidates:
        if c in df.columns and np.issubdtype(df[c].dtype, np.number):
            return df[c].astype(float)
        if c in df.columns:
            try:
                return pd.to_numeric(df[c], errors="coerce")
            except Exception:
                pass
    return None

def _match_entity(df: pd.DataFrame, entity: str) -> pd.DataFrame:
    name_cols = [c for c in df.columns if any(k in c for k in ["entity", "reference", "underlier", "name", "issuer"])]
    if not name_cols:
        return df.iloc[0:0]
    m = np.zeros(len(df), dtype=bool)
    for c in name_cols:
        m |= df[c].astype(str).map(lambda x: _contains(x, entity))
    return df[m]

def _match_tenor(df: pd.DataFrame, tenor_years: int) -> pd.DataFrame:
    # Heuristic: keep 5Y by scanning text tenor fields
    tenor_cols = [c for c in df.columns if "tenor" in c or "maturity" in c or "term" in c]
    if not tenor_cols:
        return df  # keep broad if missing
    m = np.zeros(len(df), dtype=bool)
    keys = [f"{tenor_years}y", f"{tenor_years}yr", f"{tenor_years}-year"]
    for c in tenor_cols:
        m |= df[c].astype(str).str.lower().apply(lambda x: any(k in x for k in keys))
    # If nothing matched, do not drop everything; keep all
    return df if not m.any() else df[m]

def _match_currency(df: pd.DataFrame, currency: str) -> pd.DataFrame:
    cur_cols = [c for c in df.columns if "currency" in c or c in ["ccy", "pay_ccy", "receive_ccy"]]
    if not cur_cols:
        return df
    m = np.zeros(len(df), dtype=bool)
    for c in cur_cols:
        m |= (df[c].astype(str).str.upper() == currency.upper())
    # If nothing matched, keep all
    return df if not m.any() else df[m]

def _weighted_mean(values: pd.Series, weights: Optional[pd.Series]) -> float:
    values = pd.to_numeric(values, errors="coerce")
    if weights is None:
        return float(values.mean())
    weights = pd.to_numeric(weights, errors="coerce")
    mask = values.notna() & weights.notna()
    if not mask.any():
        return float("nan")
    w = weights[mask]
    v = values[mask]
    if w.sum() == 0:
        return float(v.mean())
    return float((v * w).sum() / w.sum())

def build_series(
    start: str,
    end: str,
    entity: str,
    tenor_years: int = 5,
    currency: str = "USD",
    aggregator: str = "weighted_mean",
    raw_dir: Optional[str] = "data/raw",   # keep daily raw for debugging
) -> pd.DataFrame:
    """
    Returns daily series with columns: [date, value, count, notional_sum]
    """
    dates = pd.date_range(start=start, end=end, freq="D")
    from tqdm import tqdm

    if raw_dir:
        import pathlib
        pathlib.Path(raw_dir).mkdir(parents=True, exist_ok=True)

    agg_fn: Callable[[pd.Series, Optional[pd.Series]], float]
    if aggregator == "weighted_mean":
        agg_fn = _weighted_mean
    elif aggregator == "mean":
        agg_fn = lambda v, w: float(pd.to_numeric(v, errors="coerce").mean())
    else:
        raise ValueError(f"Unknown aggregator: {aggregator}")

    out_rows = []
    for d in tqdm(dates, desc="Dates"):
        day = d.date().isoformat()
        try:
            df = fetch_sbsdr_day(day)
        except Exception as e:
            logging.warning(f"{day}: fetch error: {e}")
            continue

        if df.empty:
            # still emit a record with NaN value (helps see gaps)
            out_rows.append({"date": day, "value": np.nan, "count": 0, "notional_sum": 0.0})
            continue

        # Keep Credit/ CDS if columns present
        if "asset_class" in df.columns:
            df = df[df["asset_class"].str.lower().eq("credit")]
        # product hints
        for c in ["product", "product_id", "product_type"]:
            if c in df.columns:
                df = df[df[c].astype(str).str.lower().str.contains("cds|credit default", regex=True, na=False)]
        if df.empty:
            out_rows.append({"date": day, "value": np.nan, "count": 0, "notional_sum": 0.0})
            continue

        # Selectors
        df = _match_entity(df, entity)
        df = _match_tenor(df, tenor_years)
        df = _match_currency(df, currency)
        if df.empty:
            out_rows.append({"date": day, "value": np.nan, "count": 0, "notional_sum": 0.0})
            continue

        # Persist raw (post-filter) for offline inspection
        if raw_dir:
            df.to_csv(f"{raw_dir}/{day}.csv.gz", index=False, compression="gzip")

        price = _pick_price(df)
        notion = _pick_notional(df)
        if price is None:
            out_rows.append({"date": day, "value": np.nan, "count": len(df), "notional_sum": float(notion.sum() if notion is not None else 0.0)})
            continue

        val = agg_fn(price, notion)
        out_rows.append({
            "date": day,
            "value": val,
            "count": int(len(df)),
            "notional_sum": float(notion.sum() if notion is not None else 0.0),
        })

    ser = pd.DataFrame(out_rows)
    ser["date"] = pd.to_datetime(ser["date"]).dt.date
    ser = ser.sort_values("date")
    return ser
