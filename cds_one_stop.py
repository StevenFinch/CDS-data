# cds_one_stop.py
from __future__ import annotations
import sys, argparse, pathlib, logging
import pandas as pd
from cds_helpers.clean_aggregate import build_series

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

def cmd_probe(args):
    from cds_helpers.sbsdr_fetch import fetch_sbsdr_day
    df = fetch_sbsdr_day(args.date, raw_dir=args.raw_dir)
    if df is None or df.empty:
        print("EMPTY")
    else:
        print(df.head(10).to_string(index=False))

def cmd_fetch(args):
    ts = build_series(
        start_date=args.start,
        end_date=args.end,
        entity=args.entity,
        tenor_years=args.tenor_years,
        currency=args.currency,
        aggregation=args.agg,
    )
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    ts.to_csv(args.out, index=False)
    print(f"Wrote {args.out}  rows={len(ts)}")

def main(argv=None):
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("probe", help="download + print one day (debug)")
    s1.add_argument("--date", required=True)
    s1.add_argument("--raw-dir", default="data/raw")
    s1.set_defaults(func=cmd_probe)

    s2 = sub.add_parser("fetch", help="download & aggregate a date range")
    s2.add_argument("--entity", required=True)
    s2.add_argument("--tenor-years", type=int, default=5)
    s2.add_argument("--currency", default="USD")
    s2.add_argument("--start", required=True)
    s2.add_argument("--end", required=True)
    s2.add_argument("--agg", choices=["weighted_mean","median","mean"], default="weighted_mean")
    s2.add_argument("--out", required=True)
    s2.set_defaults(func=cmd_fetch)

    args = p.parse_args(argv)
    return args.func(args)

if __name__ == "__main__":
    sys.exit(main())
