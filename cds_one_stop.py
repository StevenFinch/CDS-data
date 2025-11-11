#!/usr/bin/env python3
# cds_one_stop.py

import argparse
import datetime as dt
import pandas as pd
from cds_helpers.clean_aggregate import build_series, probe_days


def parse_args():
    p = argparse.ArgumentParser(
        description="One-stop sovereign CDS fetcher (SBSDR + Investing.com fallback)."
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    # ---- probe ----
    p_probe = sub.add_parser(
        "probe",
        help="Check which days in [start,end] actually have SBSDR rows for this entity/tenor."
    )
    p_probe.add_argument("--entity", required=True, help="Reference entity name, e.g. 'United States of America'")
    p_probe.add_argument("--tenor-years", type=float, default=5.0, help="Target tenor in years (e.g. 5)")
    p_probe.add_argument("--currency", default="USD", help="Currency, e.g. USD")
    p_probe.add_argument("--start", required=True, help="YYYY-MM-DD")
    p_probe.add_argument("--end", required=True, help="YYYY-MM-DD")

    # ---- fetch ----
    p_fetch = sub.add_parser(
        "fetch",
        help="Download + aggregate daily CDS series into a CSV."
    )
    p_fetch.add_argument("--entity", required=True)
    p_fetch.add_argument("--tenor-years", type=float, default=5.0)
    p_fetch.add_argument("--currency", default="USD")
    p_fetch.add_argument("--start", required=True)
    p_fetch.add_argument("--end", required=True)
    p_fetch.add_argument(
        "--agg",
        default="weighted_mean",
        choices=["weighted_mean", "mean", "median", "raw"],
        help="Daily aggregation method."
    )
    p_fetch.add_argument("--out", default=None, help="Optional CSV output path")

    return p.parse_args()


def to_date(s):
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def main():
    args = parse_args()
    start_d = to_date(args.start)
    end_d = to_date(args.end)

    if args.cmd == "probe":
        df_probe = probe_days(
            entity=args.entity,
            tenor_years=args.tenor_years,
            currency=args.currency,
            start=start_d,
            end=end_d,
        )
        # print to stdout nicely
        print(df_probe.to_string(index=False))
        return

    if args.cmd == "fetch":
        ts = build_series(
            entity=args.entity,
            tenor_years=args.tenor_years,
            currency=args.currency,
            start=start_d,
            end=end_d,
            agg=args.agg,
        )
        # show head/tail in console
        print("=== PREVIEW (head) ===")
        print(ts.head().to_string(index=False))
        print("=== PREVIEW (tail) ===")
        print(ts.tail().to_string(index=False))

        if args.out:
            ts.to_csv(args.out, index=False)
            print(f"\nSaved {len(ts)} rows to {args.out}")
        else:
            print("\n(no --out given, not saved)")

        return


if __name__ == "__main__":
    main()
