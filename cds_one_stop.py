# cds_one_stop.py
from __future__ import annotations
import argparse
import sys
import logging
import pathlib
import pandas as pd

from cds_helpers.clean_aggregate import build_series

def _parse_args(argv=None):
    p = argparse.ArgumentParser(prog="cds_one_stop.py")
    sub = p.add_subparsers(dest="cmd", required=False)

    # fetch subcommand
    pf = sub.add_parser("fetch", help="Download & aggregate ICE SBSR daily CSV into a CDS series")
    pf.add_argument("--entity", required=True, help='e.g., "United States of America"')
    pf.add_argument("--tenor-years", type=int, default=5)
    pf.add_argument("--currency", default="USD")
    pf.add_argument("--start", required=True)
    pf.add_argument("--end", required=True)
    pf.add_argument("--agg", default="weighted_mean", choices=["weighted_mean", "mean"])
    pf.add_argument("--out", required=True, help="Output CSV path")
    pf.add_argument("--raw-dir", default="data/raw", help="Where to stash filtered daily files")

    # probe subcommand (optional diagnostic)
    pp = sub.add_parser("probe", help="Only fetch one day and print column names")
    pp.add_argument("--date", required=True)

    args, unknown = p.parse_known_args(argv)

    # Backward-compat: if user didn’t provide subcommand but passed flags, assume 'fetch'
    if args.cmd is None:
        # Heuristic: the old style started with flags like --entity
        if argv and any(a.startswith("--entity") for a in argv):
            return _parse_args(["fetch"] + (argv or []))
        p.error("argument cmd is required (choose from 'probe', 'fetch')")

    return args

def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)

    if args.cmd == "probe":
        from cds_helpers.sbsdr_fetch import fetch_sbsdr_day
        df = fetch_sbsdr_day(args.date)
        print(f"Rows: {len(df)}")
        print(sorted(df.columns))
        return

    if args.cmd == "fetch":
        out = pathlib.Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        ts = build_series(
            start=args.start,
            end=args.end,
            entity=args.entity,
            tenor_years=args.tenor_years,
            currency=args.currency,
            aggregator=args.agg,
            raw_dir=args.raw_dir,
        )
        # Keep even NaN days so you can see gaps; you may dropna later if you want
        ts.rename(columns={"value": "cds_spread"}, inplace=True)
        ts.to_csv(out, index=False)
        logging.info(f"Wrote {out} ({len(ts)} rows; non-null={ts['cds_spread'].notna().sum()})")
        return

if __name__ == "__main__":
    main(sys.argv[1:])
