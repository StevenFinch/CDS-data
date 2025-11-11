# cds_helpers/sbsdr_fetch.py

import io
import datetime as dt
import pandas as pd
import requests
from .aliases import tenor_close_enough

SBSDR_BASE = (
    # ICE Trade Vault / SEC SBSDR public daily trade report path.
    # NOTE: this URL pattern may change; update here instead of everywhere.
    # Placeholder format string: {date} like "2024-01-02"
    "https://regreporting.theice.com/trade-reporting/api/v1/public-data/"
    "sbs-transaction-csv?tradeDate={date}"
)

def _safe_date(x):
    try:
        return dt.datetime.strptime(x.split("T")[0], "%Y-%m-%d").date()
    except Exception:
        return None

def fetch_sbsdr_day(date_str: str) -> pd.DataFrame:
    """
    Pull one day's CSV from SBSDR (ICE Trade Vault).
    Return as DataFrame with raw columns.
    If 404 / empty -> empty df.
    """
    url = SBSDR_BASE.format(date=date_str)
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200 or not resp.text.strip():
        return pd.DataFrame()

    # ICE gives CSV text. We'll read directly.
    raw_csv = io.StringIO(resp.text)
    try:
        df = pd.read_csv(raw_csv)
    except Exception:
        return pd.DataFrame()

    # Normalize column names to lowercase snake-ish for safety
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def filter_cds_rows(
    df: pd.DataFrame,
    entity_aliases_lower: list[str],
    ccy: str,
    tenor_years: float,
):
    """
    Keep only trades that look like sovereign CDS on that reference entity.
    We try:
    - product / asset_class must hint CDS or credit
    - currency must match
    - reference entity string must match alias
    - tenor ~ desired
    We also extract numeric spread/upfront/etc when possible.
    """
    if df.empty:
        return pd.DataFrame()

    ccy_low = (ccy or "").strip().lower()

    # Heuristic column guesses (ICE schema tends to include):
    #  - 'reference_entity', 'referenceentity', 'entity'
    #  - 'price', 'spread', 'coupon', 'fixed_rate'
    #  - 'notional_amount', 'notional', 'quantity'
    #  - 'currency', 'notional_currency'
    #  - 'effective_date', 'maturity_date'
    #  - 'product_type', 'asset_class', etc.
    colmap = {c: c for c in df.columns}  # identity map

    # unify some columns (best-effort)
    def pick(cols):
        for cand in cols:
            if cand in df.columns:
                return cand
        return None

    col_entity = pick(
        ["reference_entity", "referenceentity", "entity", "underlying_reference_entity"]
    )
    col_ccy = pick(["currency", "notional_currency", "price_currency"])
    col_prod = pick(["product_type", "product", "asset_class", "assetclass"])
    col_notional = pick(["notional_amount", "notional", "quantity", "trade_notional"])
    col_spread = pick(["spread", "spread_bps", "cds_spread"])
    col_price = pick(["price", "execution_price", "deal_price"])
    col_coupon = pick(["coupon", "fixed_rate", "running_coupon"])
    col_eff = pick(["effective_date", "start_date", "effective_datetime"])
    col_mat = pick(["maturity_date", "termination_date"])
    col_side = pick(["direction", "buy_sell"])

    work = df.copy()

    # lowercase entity for matching
    if col_entity:
        work["__entity_low__"] = work[col_entity].astype(str).str.lower()
    else:
        work["__entity_low__"] = ""

    # currency match
    if col_ccy:
        work["__ccy_low__"] = work[col_ccy].astype(str).str.lower()
    else:
        work["__ccy_low__"] = ""

    # parse dates for tenor calc
    work["__eff_date__"] = work[col_eff].astype(str).apply(_safe_date) if col_eff else None
    work["__mat_date__"] = work[col_mat].astype(str).apply(_safe_date) if col_mat else None

    # keep only CDS-like rows
    def looks_like_cds(row):
        prod_val = (row[col_prod].lower() if col_prod and isinstance(row[col_prod], str) else "")
        # require "cds" / "credit default swap"
        if ("cds" not in prod_val) and ("credit default swap" not in prod_val):
            return False

        # entity alias match
        ent_ok = row["__entity_low__"] in entity_aliases_lower

        # currency match (if we even have ccy)
        ccy_ok = True
        if ccy_low:
            ccy_ok = (row["__ccy_low__"] == ccy_low)

        # tenor check
        tenor_ok = tenor_close_enough(
            tenor_years,
            row["__eff_date__"],
            row["__mat_date__"],
            tol_years=1.0,
        )

        return ent_ok and ccy_ok and tenor_ok

    work = work[work.apply(looks_like_cds, axis=1)].copy()
    if work.empty:
        return work

    # extract numeric fields robustly
    def to_num(x):
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            return None

    work["notional"] = work[col_notional].apply(to_num) if col_notional else None
    work["spread_bps"] = work[col_spread].apply(to_num) if col_spread else None
    work["price"] = work[col_price].apply(to_num) if col_price else None
    work["coupon_bps"] = (
        work[col_coupon].apply(lambda x: to_num(x) * 10000.0 if (to_num(x) and to_num(x) < 1) else to_num(x))
        if col_coupon
        else None
    )
    # crude heuristic: if coupon is e.g. 0.05 (5%), convert to 500 bps. If already ~500, leave ~500.

    # direction
    if col_side:
        work["side"] = work[col_side].astype(str).str.lower()
    else:
        work["side"] = None

    # choose "best available quote" per row:
    # preference: spread_bps, then price, then coupon_bps
    def choose_quote(row):
        if row["spread_bps"] is not None:
            return row["spread_bps"], "spread_bps"
        if row["price"] is not None:
            return row["price"], "price"
        if row["coupon_bps"] is not None:
            return row["coupon_bps"], "running_coupon_bps"
        return None, None

    quotes = work.apply(choose_quote, axis=1)
    work["quote_value_bps"] = [q[0] for q in quotes]
    work["quote_type"] = [q[1] for q in quotes]

    return work[
        [
            col_entity if col_entity else "__entity_low__",
            "notional",
            "spread_bps",
            "price",
            "coupon_bps",
            "quote_value_bps",
            "quote_type",
            col_eff if col_eff else "__eff_date__",
            col_mat if col_mat else "__mat_date__",
            "side",
        ]
    ].copy()
