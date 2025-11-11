# cds_one_stop.py
from __future__ import annotations
import argparse
import logging
import sys
from datetime import date, datetime

import pandas as pd
from cds_helpers.clean_aggregate import build_series, _MIN_SBSR_DATE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def cmd_probe(args: argparse.Namespace) -> None:
    print("SBSR coverage notice:")
    print(f"  - Security-based swaps public dissemination (SBSR) began on {_MIN_SBSR_DATE}.")
    print("  - If you request dates prior to that, the series will return NaNs for those days.")
    print("Entities & selectors are heuristic; inspect 'data/debug' if you get unexpected empties.")

def cmd_fetch(args: argparse.Namespace) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)

    ser = build_series(
        start=start,
        end=end,
        entity=args.entity,
        tenor_years=args.tenor_years,
        currency=args.currency,
        agg=args.agg,
        min_start=_MIN_SBSR_DATE,  # clip to SBSR go-live
    )
    out = args.out
    ser.rename(columns={"value_bps": "cds_5y_bps"}, inplace=True)
    ser.to_csv(out, index=False)
    logging.info("Wrote %s (%d rows, %d non-NaN)", out, len(ser), ser["cds_5y_bps"].notna().sum())

def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser(prog="cds_one_stop.py")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("probe", help="Explain data coverage & selectors")
    pr.set_defaults(func=cmd_probe)

    f = sub.add_parser("fetch", help="Fetch & aggregate CDS series")
    f.add_argument("--entity", required=True, help='e.g., "United States of America"')
    f.add_argument("--tenor-years", type=int, default=5)
    f.add_argument("--currency", default="USD")
    f.add_argument("--start", required=True)
    f.add_argument("--end", required=True)
    f.add_argument("--agg", choices=["weighted_mean", "mean"], default="weighted_mean")
    f.add_argument("--out", required=True)
    f.set_defaults(func=cmd_fetch)

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main(sys.argv[1:])
