# cds_helpers/clean_aggregate.py

import datetime as dt
import pandas as pd
from tqdm import tqdm
from .sbsdr_fetch import fetch_sbsdr_day, filter_cds_rows
from .investing_fetch import fetch_investing_history
from .aliases import default_aliases_for_entity


def daterange(start_date: dt.date, end_date: dt.date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += dt.timedelta(days=1)


def aggregate_day(df_day: pd.DataFrame, agg: str):
    """
    Given filtered intraday rows for a single day, compute a single representative value.

    agg:
      - 'weighted_mean' : weight by notional (if available)
      - 'mean'
      - 'median'
      - 'raw' : return None here; caller can keep intraday rows

    Returns (value_bps, quote_type_mode, total_notional)
    or (None, None, 0) if df_day empty or no usable quotes.
    """
    if df_day is None or df_day.empty:
        return None, None, 0.0

    work = df_day.copy()
    work = work[pd.notnull(work["quote_value_bps"])]
    if work.empty:
        return None, None, 0.0

    work["notional_clean"] = work["notional"].fillna(0.0)
    # mode for quote_type for reporting
    quote_type_mode = (
        work["quote_type"]
        .fillna("unknown")
        .mode()
        .iat[0]
        if not work["quote_type"].empty
        else "unknown"
    )

    if agg == "weighted_mean":
        # If all notionals are zero, fallback to simple mean
        total_notional = work["notional_clean"].sum()
        if total_notional > 0:
            val = (work["quote_value_bps"] * work["notional_clean"]).sum() / total_notional
        else:
            val = work["quote_value_bps"].mean()
    elif agg == "mean":
        total_notional = work["notional_clean"].sum()
        val = work["quote_value_bps"].mean()
    elif agg == "median":
        total_notional = work["notional_clean"].sum()
        val = work["quote_value_bps"].median()
    elif agg == "raw":
        # caller doesn't want daily collapse
        # we just tell caller "no single number"
        total_notional = work["notional_clean"].sum()
        return None, quote_type_mode, total_notional
    else:
        raise ValueError(f"Unknown agg {agg}")

    return float(val), quote_type_mode, float(total_notional)


def build_series(
    entity: str,
    tenor_years: float,
    currency: str,
    start: dt.date,
    end: dt.date,
    agg: str,
):
    """
    Loop over calendar days in [start,end], try SBSDR per day,
    fallback to Investing.com if SBSDR returns nothing for that day.

    Return:
        ts_df: DataFrame with columns
          ['date','cds_bps','source','quote_type','total_notional']
    """

    aliases = default_aliases_for_entity(entity)
    investing_hist = fetch_investing_history()

    rows = []
    for d in tqdm(list(daterange(start, end)), desc="Dates"):
        day_str = d.strftime("%Y-%m-%d")

        # 1. SBSDR try
        raw = fetch_sbsdr_day(day_str)
        filt = filter_cds_rows(
            raw,
            entity_aliases_lower=aliases,
            ccy=currency,
            tenor_years=tenor_years,
        )
        v_bps, qtype, notion = aggregate_day(filt, agg=agg)

        if v_bps is not None:
            rows.append(
                {
                    "date": d,
                    "cds_bps": v_bps,
                    "source": "SBSDR",
                    "quote_type": qtype,
                    "total_notional": notion,
                }
            )
            continue

        # 2. fallback to Investing.com for that calendar date
        # We just look for exact date match
        if not investing_hist.empty:
            m = investing_hist[investing_hist["date"] == d]
            if not m.empty:
                val = float(m["cds_bps"].iloc[0])
                rows.append(
                    {
                        "date": d,
                        "cds_bps": val,
                        "source": "Investing.com",
                        "quote_type": "last_print",
                        "total_notional": None,
                    }
                )
                continue

        # 3. neither SBSDR nor Investing.com has data that day
        rows.append(
            {
                "date": d,
                "cds_bps": None,
                "source": None,
                "quote_type": None,
                "total_notional": 0.0,
            }
        )

    ts = pd.DataFrame(rows)
    ts = ts.sort_values("date").reset_index(drop=True)
    return ts


def probe_days(
    entity: str,
    tenor_years: float,
    currency: str,
    start: dt.date,
    end: dt.date,
):
    """
    For debugging: tell you which days have SBSDR hits.
    We DO NOT aggregate, just tell you length of filt per day.
    """

    aliases = default_aliases_for_entity(entity)
    out = []
    for d in daterange(start, end):
        day_str = d.strftime("%Y-%m-%d")
        raw = fetch_sbsdr_day(day_str)
        filt = filter_cds_rows(
            raw,
            entity_aliases_lower=aliases,
            ccy=currency,
            tenor_years=tenor_years,
        )
        out.append(
            {
                "date": d,
                "rows_found": len(filt),
                "first_quote_type": (filt["quote_type"].iloc[0] if len(filt) else None),
            }
        )
    return pd.DataFrame(out)
