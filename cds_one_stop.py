#!/usr/bin/env python3
import argparse
import sys
import pandas as pd
from cds_helpers.clean_aggregate import build_series
from cds_helpers.sbsdr_fetch import fetch_sbsdr_day
from cds_helpers.parsing import find_entity_column, find_tenor_column

def cmd_probe(args):
    df = fetch_sbsdr_day(args.date)
    if df.empty:
        print(f"[probe] No data for {args.date} (holiday or pre-2022-02-14).")
        return
    ent_col = find_entity_column(df) or "(not found)"
    ten_col = find_tenor_column(df) or "(not found)"
    print(f"[probe] Columns: {len(df.columns)}; entity_col={ent_col}; tenor_col={ten_col}")
    if ent_col != "(not found)":
        top = (df[ent_col].astype(str)
               .value_counts(dropna=True)
               .head(30))
        print("\n[probe] Top 30 entity/underlier-like values:")
        for k, v in top.items():
            print(f"  {v:5d} × {k}")
    print("\n[probe] First 5 rows (head):")
    print(df.head().to_string(index=False))

def cmd_fetch(args):
    ts = build_series(
        entity_name=args.entity,
        start=args.start,
        end=args.end,
        currency=args.currency,
        tenor_prefer=str(args.tenor_years),
        agg=args.agg,
    )
    if ts.empty:
        print("No data collected in the specified range "
              "(check: public dissemination starts 2022-02-14; entity label; holiday).")
        sys.exit(0)
    out = args.out
    ts.to_csv(out, index=False)
    print(f"[ok] wrote {len(ts)} rows -> {out}")

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("probe", help="Inspect a single day to discover columns/entity labels")
    sp.add_argument("--date", required=True, help="ISO date, e.g., 2024-06-03")
    sp.set_defaults(func=cmd_probe)

    sf = sub.add_parser("fetch", help="Build a daily series for a range and write CSV")
    sf.add_argument("--entity", required=True, help='e.g., "United States of America"')
    sf.add_argument("--tenor-years", type=int, default=5)
    sf.add_argument("--currency", default="USD")
    sf.add_argument("--start", required=True)
    sf.add_argument("--end", required=True)
    sf.add_argument("--agg", choices=["weighted_mean", "mean"], default="weighted_mean")
    sf.add_argument("--out", required=True)
    sf.set_defaults(func=cmd_fetch)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
