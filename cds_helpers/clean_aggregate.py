# cds_helpers/clean_aggregate.py
from __future__ import annotations
import io, logging
from typing import Iterable, Dict, List, Optional
import pandas as pd
from tqdm import tqdm
from .sbsdr_fetch import fetch_sbsdr_day

LOG = logging.getLogger("SBSR")

ENTITY_ALIASES = {
    # normalized lower -> set of acceptable names observed in feeds
    "united states of america": {
        "united states of america", "united states", "usa", "u.s.", "u.s.a.", "us"
    },
}

def _read_csv_loose(text: str) -> pd.DataFrame:
    if not text:
        return pd.DataFrame()
    # Robust CSV load: unknown delimiters sometimes appear; ICE uses commas
    try:
        df = pd.read_csv(io.StringIO(text), dtype=str)
    except Exception:
        df = pd.read_csv(io.StringIO(text), dtype=str, sep=",", engine="python", on_bad_lines="skip")
    # unify columns to lowercase for probing
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def _pick_col(df: pd.DataFrame, cands: Iterable[str]) -> Optional[str]:
    for c in cands:
        if c in df.columns:
            return c
    return None

def _normalize_entity(x: str) -> str:
    return (x or "").strip().lower()

def _filter_daily(
    df: pd.DataFrame,
    entity: str,
    currency: str,
    tenor_years: int
) -> pd.DataFrame:
    if df.empty:
        return df

    # columns we may see (varies slightly by vendor version)
    entity_cols = [
        "referenceentity", "reference_entity", "underlyingreferencename",
        "underlyingreference", "underlyingentityname", "underlying_name",
        "entity", "reference_name"
    ]
    asset_cols = ["assetclass", "asset_class", "product", "producttype", "securitybasedswap"]
    cur_cols   = ["currency", "notionalcurrency", "pricecurrency", "settlementcurrency"]
    tenor_cols = ["tenor", "maturitytenor", "underlyingtenor", "maturity_tenor"]
    price_cols = [
        "spread", "fixedrate", "price", "pricenotation", "price_notation", "pricenotationvalue", "pricevalue"
    ]
    ptype_cols = ["pricenotationtype", "price_notation_type", "pricetype"]

    ent_col = _pick_col(df, entity_cols)
    if not ent_col:
        return pd.DataFrame()

    # Entity filter via aliases
    want = _normalize_entity(entity)
    names = ENTITY_ALIASES.get(want, {want})
    df = df[df[ent_col].str.lower().isin(names)]

    if df.empty:
        return df

    # Asset must look like CDS
    ac = _pick_col(df, asset_cols)
    if ac:
        df = df[df[ac].str.contains("cds", case=False, na=False)]
        if df.empty:
            return df

    # Currency filter (if present)
    cc = _pick_col(df, cur_cols)
    if cc:
        df = df[df[cc].str.upper() == currency.upper()]
        if df.empty:
            return df

    # Tenor: accept explicit 5Y in any tenor column
    tcol = _pick_col(df, tenor_cols)
    if tcol:
        df = df[df[tcol].str.upper().str.contains(f"{tenor_years}Y", na=False)]
        # allow empty -> keep all (some prints omit tenor)
        if df.empty:
            return df

    # Price value & type
    pcol = _pick_col(df, price_cols)
    tptype = _pick_col(df, ptype_cols)
    if pcol is None:
        return pd.DataFrame()

    # keep rows where price type indicates SPREAD/BPS or where field named 'spread'/'fixedrate'
    if tptype in df.columns:
        mask = df[tptype].str.contains("SPREAD|BPS|BP|BASIS", case=False, na=False)
        df2 = df[mask].copy()
        if not df2.empty:
            df = df2

    # Weight: use notional when possible
    wcol = _pick_col(df, ["notional", "reportednotional", "notionalamount", "reported_notional"])
    if wcol and wcol in df.columns:
        # coerce numeric
        df["_w"] = pd.to_numeric(df[wcol].str.replace(",", ""), errors="coerce")
    else:
        df["_w"] = 1.0

    df["_x"] = pd.to_numeric(df[pcol].str.replace(",", ""), errors="coerce")  # spread/fixedrate/price
    df = df[df["_x"].notna()]

    return df[["_x", "_w"]]

def _aggregate(d: pd.DataFrame, agg: str) -> Optional[float]:
    if d is None or d.empty:
        return None
    if agg == "weighted_mean":
        w = d["_w"].fillna(0)
        x = d["_x"].fillna(0)
        s = (w * x).sum()
        ws = w.sum()
        if ws == 0:
            return float(x.mean()) if len(x) else None
        return float(s / ws)
    elif agg == "median":
        return float(d["_x"].median())
    elif agg == "mean":
        return float(d["_x"].mean())
    else:
        return float(d["_x"].mean())

def build_series(
    start: str,
    end: str,
    *,
    entity: str,
    currency: str,
    tenor_years: int,
    agg: str = "weighted_mean",
) -> pd.DataFrame:
    """
    Returns a tidy daily dataframe with columns: date, value_bps, count, weight_sum
    """
    days = pd.date_range(start=start, end=end, freq="D")
    out_rows: List[Dict] = []

    for day in tqdm(days, desc="Dates"):
        dstr = day.strftime("%Y-%m-%d")
        try:
            raw = fetch_sbsdr_day(dstr)
        except Exception as e:
            LOG.warning("%s: fetch error: %s", dstr, e)
            continue

        df = _read_csv_loose(raw)
        dd = _filter_daily(df, entity=entity, currency=currency, tenor_years=tenor_years)
        val = _aggregate(dd, agg=agg)
        if val is None:
            continue

        out_rows.append({
            "date": dstr,
            "value_bps": val,
            "count": int(len(dd)),
            "weight_sum": float(dd["_w"].sum()) if (dd is not None and not dd.empty) else 0.0,
        })

    if not out_rows:
        return pd.DataFrame(columns=["date", "value_bps", "count", "weight_sum"])

    ser = pd.DataFrame(out_rows)
    ser["date"] = pd.to_datetime(ser["date"]).dt.date
    ser = ser.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return ser
