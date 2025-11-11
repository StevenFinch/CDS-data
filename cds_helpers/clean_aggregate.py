import io, re, datetime as dt
import pandas as pd
from tqdm import tqdm
from cds_helpers.sbsdr_fetch import fetch_sbsdr_day

# --------- Heuristic field maps (case-insensitive) ----------
ENTITY_FIELDS = [
    "reference_entity","referenceentity","referenceentityname","reference_entity_name",
    "underliername","underlier","underlyingname","underlying_name","entity"
]
TENOR_FIELDS = ["tenor","maturity_tenor","maturitytenor","cds_tenor"]
CURR_FIELDS  = ["currency","notional_currency","price_currency","upfront_currency"]
PRICE_FIELDS = ["spread","price","traded_price","price_notation","pricenotation"]
WEIGHT_FIELDS = ["notional","notional_amount","trade_notional_amount","size","quantity","qty","notionalamount"]

def _first_col(df: pd.DataFrame, cands: list[str]):
    cols = {c.lower(): c for c in df.columns}
    for k in cands:
        if k in cols: return cols[k]
    # try contains search
    for k in cands:
        for c in df.columns:
            if k in c.lower(): return c
    return None

def _parse_price(row) -> float | None:
    # Prefer explicit spread
    for k in PRICE_FIELDS:
        val = row.get(k)
        if val is None: continue
        try:
            x = float(str(val).replace(',', '').strip())
            # Many SBSDR CDS quotes are in bps already; keep as-is
            return x
        except:  # noqa
            continue
    return None

def _parse_weight(row) -> float | None:
    for k in WEIGHT_FIELDS:
        val = row.get(k)
        if val is None: continue
        try:
            w = float(str(val).replace(',', '').strip())
            if w > 0: return w
        except:  # noqa
            continue
    return None

def _tenor_matches(tenor_text: str | None, want_years: int) -> bool:
    if not tenor_text:  # unknown -> accept
        return True
    t = tenor_text.upper().strip()
    # direct like "5Y" or "60M"
    if re.fullmatch(rf"{want_years}\s*Y", t): return True
    m = re.fullmatch(r"(\d+)\s*M", t)
    if m:
        return abs(int(m.group(1)) - want_years*12) <= 6
    # forms like "SNRFOR-5Y" or "CDS-5Y"
    if f"{want_years}Y" in t: return True
    return False

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().lower()

def _entity_matches(val: str | None, want: str) -> bool:
    if not val: return False
    return _norm(val) == _norm(want)

def _currency_matches(val: str | None, want: str) -> bool:
    if not val: return True  # accept if missing
    return _norm(val) == _norm(want)

def build_series(start: dt.date, end: dt.date,
                 entity: str, tenor_years: int, currency: str,
                 agg: str = "weighted_mean") -> pd.DataFrame:
    """
    Loops day by day, fetches ICE SBSDR CSV, filters to (entity, tenor, currency),
    and aggregates to a daily time-series in bps.
    """
    dates = pd.date_range(start=start, end=end, freq="D")
    rows = []

    for day in tqdm(dates, desc="Dates"):
        dstr = day.strftime("%Y-%m-%d")
        try:
            txt = fetch_sbsdr_day(dstr)
        except Exception:
            continue
        if not txt.strip():
            continue

        try:
            df = pd.read_csv(io.StringIO(txt))
        except Exception:
            # Sometimes the file is headerless / malformed; skip
            continue

        # Lowercase map for flexible access
        df.columns = [c.strip() for c in df.columns]
        lower_map = {c.lower(): c for c in df.columns}

        # entity
        ent_col = _first_col(df, ENTITY_FIELDS)
        # tenor
        ten_col = _first_col(df, TENOR_FIELDS)
        # currency
        ccy_col = _first_col(df, CURR_FIELDS)

        # flexible select
        filt = pd.Series([True] * len(df))
        if ent_col:
            filt &= df[ent_col].astype(str).map(lambda x: _entity_matches(x, entity))
        else:
            # if we can't find a clear entity column, skip day
            continue

        if ten_col:
            filt &= df[ten_col].astype(str).map(lambda x: _tenor_matches(x, tenor_years))

        if ccy_col:
            filt &= df[ccy_col].astype(str).map(lambda x: _currency_matches(x, currency))

        sub = df[filt].copy()
        if sub.empty:
            continue

        # price + weight extraction
        # create lower-keyed dict per row
        def row_dict(sr):
            return {k.lower(): sr[k] for k in sr.index}

        prices, weights = [], []
        for _, sr in sub.iterrows():
            rd = row_dict(sr)
            p = _parse_price(rd)
            if p is None:
                continue
            w = _parse_weight(rd)
            if w is None:
                w = 1.0
            prices.append(float(p))
            weights.append(float(w))

        if not prices:
            continue

        import numpy as np
        if agg == "weighted_mean":
            v = float(np.average(prices, weights=weights))
        elif agg == "mean":
            v = float(np.mean(prices))
        elif agg == "median":
            v = float(np.median(prices))
        else:  # last
            v = float(prices[-1])

        rows.append({
            "date": dstr,
            "entity": entity,
            "tenor_years": tenor_years,
            "currency": currency,
            "cds_bps": v,
            "prints": len(prices),
            "weight_sum": float(sum(weights)),
            "source": "ICE_SBSDR"
        })

    return pd.DataFrame(rows)
